/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package filerestore

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	k8sv1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	v1 "kubevirt.io/api/core/v1"
	filerestorev1alpha1 "kubevirt.io/api/filerestore/v1alpha1"
	guestcommandv1alpha1 "kubevirt.io/api/guestcommand/v1alpha1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"
	cdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"

	"kubevirt.io/kubevirt/pkg/apimachinery/patch"
	storagetypes "kubevirt.io/kubevirt/pkg/storage/types"
)

const (
	fileRestoreFinalizer   = "kubevirt.io/file-restore-cleanup"
	unexpectedResourceFmt  = "unexpected resource %+v"
	failedKeyFromObjectFmt = "failed to get key from object: %v, %v"

	// fileRestoreScriptPath is the path to the filerestore.sh script inside Linux guests
	fileRestoreScriptPath = "/usr/local/bin/filerestore.sh"

	// fileRestoreScriptPathWindows is the path to the filerestore.bat script inside Windows guests
	fileRestoreScriptPathWindows = `"C:\Program Files\filerestore\filerestore.bat"`

	// mountPath is the path where the restore volume is mounted inside Linux guests
	mountPath = "/backup"

	// mountPathWindows is the path where the restore volume is mounted inside Windows guests
	mountPathWindows = `C:\backup`
)

// ErrStandaloneVMINotSupported is returned when attempting to hotplug to a VMI without an owning VM
var ErrStandaloneVMINotSupported = fmt.Errorf("standalone VMI not supported: VMI must be owned by a VirtualMachine for declarative hotplug")

func isManualMode(sourcePath string) bool {
	return sourcePath == ""
}

func getVMIFromInformer(vmiInformer cache.SharedIndexInformer, namespace, name string) (*v1.VirtualMachineInstance, error) {
	key := fmt.Sprintf("%s/%s", namespace, name)
	obj, exists, err := vmiInformer.GetStore().GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, k8serrors.NewNotFound(v1.Resource("virtualmachineinstance"), name)
	}
	return obj.(*v1.VirtualMachineInstance).DeepCopy(), nil
}

// isVolumeReady checks if a hotplugged volume is fully ready in the VMI
func isVolumeReady(vmi *v1.VirtualMachineInstance, volumeName string) (bool, string) {
	for _, volumeStatus := range vmi.Status.VolumeStatus {
		if volumeStatus.Name == volumeName {
			if volumeStatus.HotplugVolume != nil &&
				volumeStatus.Phase == v1.VolumeReady &&
				volumeStatus.Target != "" {
				return true, volumeStatus.Target
			}
			log.Log.Infof("Volume %s status: phase=%s, target=%s, hotplugVolume=%v",
				volumeName, volumeStatus.Phase, volumeStatus.Target, volumeStatus.HotplugVolume != nil)
			return false, ""
		}
	}
	return false, ""
}

// hotplugPVCToVM hotplugs a PVC to a VM using declarative hotplug.
// Returns ErrStandaloneVMINotSupported if VMI is not owned by a VM.
func hotplugPVCToVM(client kubecli.KubevirtClient, vmi *v1.VirtualMachineInstance, pvcName, volumeName string, readOnly bool) error {
	logger := log.Log.Object(vmi)

	vmName := getOwnerVM(vmi)
	if vmName == "" {
		logger.Errorf("VMI %s/%s is not owned by a VM - standalone VMIs are not supported for declarative hotplug", vmi.Namespace, vmi.Name)
		return ErrStandaloneVMINotSupported
	}

	vm, err := client.VirtualMachine(vmi.Namespace).Get(context.Background(), vmName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get VM %s: %w", vmName, err)
	}

	vmVolumesByName := storagetypes.GetVolumesByName(&vm.Spec.Template.Spec)
	if _, exists := vmVolumesByName[volumeName]; exists {
		logger.Infof("Volume %s already exists in VM %s spec, skipping hotplug", volumeName, vmName)
		return nil
	}

	logger.Infof("Adding declarative hotplug volume %s (pvc=%s, readOnly=%v) to VM %s", volumeName, pvcName, readOnly, vmName)

	newVolume := v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: k8sv1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
					ReadOnly:  readOnly,
				},
				Hotpluggable: true,
			},
		},
	}

	newDisk := v1.Disk{
		Name: volumeName,
		DiskDevice: v1.DiskDevice{
			Disk: &v1.DiskTarget{
				Bus:      v1.DiskBusSCSI,
				ReadOnly: readOnly,
			},
		},
		Serial: volumeName,
	}

	newVolumes := append(vm.Spec.Template.Spec.Volumes, newVolume)
	newDisks := append(vm.Spec.Template.Spec.Domain.Devices.Disks, newDisk)

	patchSet := patch.New(
		patch.WithTest("/spec/template/spec/volumes", vm.Spec.Template.Spec.Volumes),
		patch.WithTest("/spec/template/spec/domain/devices/disks", vm.Spec.Template.Spec.Domain.Devices.Disks),
		patch.WithReplace("/spec/template/spec/volumes", newVolumes),
		patch.WithReplace("/spec/template/spec/domain/devices/disks", newDisks),
	)

	patchBytes, err := patchSet.GeneratePayload()
	if err != nil {
		return fmt.Errorf("failed to generate patch payload: %w", err)
	}

	_, err = client.VirtualMachine(vm.Namespace).Patch(context.Background(), vm.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		logger.Reason(err).Errorf("Failed to patch VM %s to add hotplug volume %s", vmName, volumeName)
		return fmt.Errorf("failed to patch VM %s: %w", vmName, err)
	}

	logger.Infof("Successfully patched VM %s to add declarative hotplug volume %s (pvc=%s)", vmName, volumeName, pvcName)
	return nil
}

// unplugVolumeFromVM removes a hotplugged volume from a VM using declarative hotplug.
// Returns ErrStandaloneVMINotSupported if VMI is not owned by a VM.
func unplugVolumeFromVM(client kubecli.KubevirtClient, vmi *v1.VirtualMachineInstance, volumeName string) error {
	logger := log.Log.Object(vmi)

	vmName := getOwnerVM(vmi)
	if vmName == "" {
		logger.Errorf("VMI %s/%s is not owned by a VM - standalone VMIs are not supported for declarative hotplug", vmi.Namespace, vmi.Name)
		return ErrStandaloneVMINotSupported
	}

	vm, err := client.VirtualMachine(vmi.Namespace).Get(context.Background(), vmName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get VM %s: %w", vmName, err)
	}

	vmVolumesByName := storagetypes.GetVolumesByName(&vm.Spec.Template.Spec)
	if _, exists := vmVolumesByName[volumeName]; !exists {
		logger.Infof("Volume %s does not exist in VM %s spec, nothing to unplug", volumeName, vmName)
		return nil
	}

	logger.Infof("Removing declarative hotplug volume %s from VM %s", volumeName, vmName)

	newVolumes := make([]v1.Volume, 0, len(vm.Spec.Template.Spec.Volumes))
	for _, vol := range vm.Spec.Template.Spec.Volumes {
		if vol.Name != volumeName {
			newVolumes = append(newVolumes, vol)
		}
	}

	newDisks := make([]v1.Disk, 0, len(vm.Spec.Template.Spec.Domain.Devices.Disks))
	for _, disk := range vm.Spec.Template.Spec.Domain.Devices.Disks {
		if disk.Name != volumeName {
			newDisks = append(newDisks, disk)
		}
	}

	patchSet := patch.New(
		patch.WithTest("/spec/template/spec/volumes", vm.Spec.Template.Spec.Volumes),
		patch.WithTest("/spec/template/spec/domain/devices/disks", vm.Spec.Template.Spec.Domain.Devices.Disks),
		patch.WithReplace("/spec/template/spec/volumes", newVolumes),
		patch.WithReplace("/spec/template/spec/domain/devices/disks", newDisks),
	)

	patchBytes, err := patchSet.GeneratePayload()
	if err != nil {
		return fmt.Errorf("failed to generate patch payload: %w", err)
	}

	_, err = client.VirtualMachine(vm.Namespace).Patch(context.Background(), vm.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		logger.Reason(err).Errorf("Failed to patch VM %s to remove hotplug volume %s", vmName, volumeName)
		return fmt.Errorf("failed to patch VM %s: %w", vmName, err)
	}

	logger.Infof("Successfully patched VM %s to remove declarative hotplug volume %s", vmName, volumeName)
	return nil
}

func getOwnerVM(vmi *v1.VirtualMachineInstance) string {
	owner := metav1.GetControllerOf(vmi)
	if owner != nil && owner.Kind == "VirtualMachine" && owner.APIVersion == v1.SchemeGroupVersion.String() {
		return owner.Name
	}
	return ""
}

func isVolumeAttached(vmi *v1.VirtualMachineInstance, volumeName, pvcName string) bool {
	for _, volume := range vmi.Spec.Volumes {
		if volume.Name == volumeName {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvcName {
				return true
			}
		}
	}
	return false
}

func guestCommandName(crName string) string {
	return fmt.Sprintf("%s-cmd", crName)
}

// guestCommandShell returns the Command slice to execute a shell command on the guest,
// using the appropriate shell for the guest OS.
func guestCommandShell(vmi *v1.VirtualMachineInstance, command string) []string {
	if isWindowsGuest(vmi) {
		return []string{ /*"cmd.exe", "/c", */ command}
	}
	return []string{"/bin/sh", "-c", command}
}

// isWindowsGuest returns true if the VMI is running a Windows guest OS.
// Checks the vm.kubevirt.io/os annotation first (set by preferences, available before boot),
// then falls back to GuestOSInfo from the QEMU guest agent.
func isWindowsGuest(vmi *v1.VirtualMachineInstance) bool {
	if os, ok := vmi.Annotations["vm.kubevirt.io/os"]; ok {
		return strings.HasPrefix(strings.ToLower(os), "windows")
	}
	if vmi.Status.GuestOSInfo.Name != "" {
		return strings.Contains(strings.ToLower(vmi.Status.GuestOSInfo.Name), "windows")
	}
	return false
}

func getScriptPath(vmi *v1.VirtualMachineInstance) string {
	if isWindowsGuest(vmi) {
		return fileRestoreScriptPathWindows
	}
	return fileRestoreScriptPath
}

// getMountPath returns the guest-side mount path for the restore volume based on the OS.
func getMountPath(vmi *v1.VirtualMachineInstance, pvcName string) string {
	if isWindowsGuest(vmi) {
		return mountPathWindows + "-" + pvcName
	}
	return mountPath + "-" + pvcName
}

// VMFileRestoreController is responsible for restoring files to VMs
type VMFileRestoreController struct {
	Client kubecli.KubevirtClient

	VMFileRestoreInformer  cache.SharedIndexInformer
	VMInformer             cache.SharedIndexInformer
	VMIInformer            cache.SharedIndexInformer
	VMGuestCommandInformer cache.SharedIndexInformer
	DataVolumeInformer     cache.SharedIndexInformer

	Recorder record.EventRecorder

	vmFileRestoreQueue workqueue.TypedRateLimitingInterface[string]
}

// Init initializes the file restore controller
func (ctrl *VMFileRestoreController) Init() error {
	ctrl.vmFileRestoreQueue = workqueue.NewTypedRateLimitingQueueWithConfig[string](
		workqueue.DefaultTypedControllerRateLimiter[string](),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: "virt-controller-filerestore-vmfilerestore"},
	)

	_, err := ctrl.VMFileRestoreInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handleVMFileRestore,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handleVMFileRestore(newObj) },
			DeleteFunc: ctrl.handleVMFileRestore,
		},
	)

	if err != nil {
		return err
	}

	_, err = ctrl.VMInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handleVM,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handleVM(newObj) },
			DeleteFunc: ctrl.handleVM,
		},
	)

	if err != nil {
		return err
	}

	// Watch VMI changes to trigger reconciliation when volume status changes
	_, err = ctrl.VMIInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handleVMI,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handleVMI(newObj) },
			DeleteFunc: ctrl.handleVMI,
		},
	)

	if err != nil {
		return err
	}

	// Watch VirtualMachineGuestCommand changes to trigger reconciliation when command status changes
	_, err = ctrl.VMGuestCommandInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handleGuestCommand,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handleGuestCommand(newObj) },
			DeleteFunc: ctrl.handleGuestCommand,
		},
	)

	if err != nil {
		return err
	}

	// Watch DataVolume changes to trigger reconciliation when snapshot-based DV status changes
	_, err = ctrl.DataVolumeInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    ctrl.handleDataVolume,
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.handleDataVolume(newObj) },
			DeleteFunc: ctrl.handleDataVolume,
		},
	)

	if err != nil {
		return err
	}

	return nil
}

// Run the controller
func (ctrl *VMFileRestoreController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer ctrl.vmFileRestoreQueue.ShutDown()

	log.Log.Info("Starting file restore controller")
	defer log.Log.Info("Shutting down file restore controller")

	if !cache.WaitForCacheSync(stopCh,
		ctrl.VMFileRestoreInformer.HasSynced,
		ctrl.VMInformer.HasSynced,
		ctrl.VMIInformer.HasSynced,
		ctrl.VMGuestCommandInformer.HasSynced,
		ctrl.DataVolumeInformer.HasSynced,
	) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	for i := 0; i < threadiness; i++ {
		go wait.Until(ctrl.runWorker, time.Second, stopCh)
	}

	<-stopCh
	return nil
}

func (ctrl *VMFileRestoreController) runWorker() {
	for ctrl.processNextWorkItem() {
	}
}

func (ctrl *VMFileRestoreController) processNextWorkItem() bool {
	obj, shutdown := ctrl.vmFileRestoreQueue.Get()
	if shutdown {
		return false
	}

	defer ctrl.vmFileRestoreQueue.Done(obj)

	if err := ctrl.syncVMFileRestore(obj); err != nil {
		log.Log.Reason(err).Infof("Error syncing file restore %q, requeuing", obj)
		ctrl.vmFileRestoreQueue.AddRateLimited(obj)
		return true
	}

	ctrl.vmFileRestoreQueue.Forget(obj)
	return true
}

func (ctrl *VMFileRestoreController) syncVMFileRestore(key string) error {
	obj, exists, err := ctrl.VMFileRestoreInformer.GetStore().GetByKey(key)
	if err != nil {
		log.Log.Reason(err).Errorf("Failed to get VirtualMachineFileRestore from store: %s", key)
		return err
	}

	if !exists {
		log.Log.V(3).Infof("VirtualMachineFileRestore %s was deleted, skipping", key)
		return nil
	}

	vmFileRestore := obj.(*filerestorev1alpha1.VirtualMachineFileRestore)
	logger := log.Log.Object(vmFileRestore)
	logger.Infof("Processing VirtualMachineFileRestore: vmiName=%s, sourcePath=%s", vmFileRestore.Spec.VMIName, vmFileRestore.Spec.SourcePath)

	// Handle deletion - clean up resources before removing finalizer
	if vmFileRestore.DeletionTimestamp != nil {
		return ctrl.handleRestoreDeletion(vmFileRestore)
	}

	// Initialize status if nil
	if vmFileRestore.Status == nil {
		vmFileRestore.Status = &filerestorev1alpha1.VirtualMachineFileRestoreStatus{}
	}

	// Set initial phase to Pending if not set
	if vmFileRestore.Status.Phase == "" {
		if err := ctrl.updateRestorePhase(vmFileRestore, filerestorev1alpha1.FileOperationPhasePending); err != nil {
			return err
		}
	}

	// Add finalizer if not present (needed for cleanup on deletion)
	if !slices.Contains(vmFileRestore.Finalizers, fileRestoreFinalizer) {
		if err := ctrl.addRestoreFinalizer(vmFileRestore); err != nil {
			return err
		}
	}

	// Get the VMI
	vmi, err := getVMIFromInformer(ctrl.VMIInformer, vmFileRestore.Namespace, vmFileRestore.Spec.VMIName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Warningf("VMI %s/%s not found, waiting for VMI to be created", vmFileRestore.Namespace, vmFileRestore.Spec.VMIName)
			return nil
		}
		return fmt.Errorf("failed to get VMI %s: %w", vmFileRestore.Spec.VMIName, err)
	}

	logger.Infof("Found VMI %s, phase=%s", vmi.Name, vmi.Status.Phase)

	if !vmi.IsRunning() {
		logger.Warningf("VMI %s is not running (phase=%s), waiting for VMI to be running", vmFileRestore.Spec.VMIName, vmi.Status.Phase)
		return nil
	}

	// Handle PVC source
	if vmFileRestore.Spec.Source.PVC != nil {
		logger.Infof("Processing PVC source: pvcName=%s", vmFileRestore.Spec.Source.PVC.Name)
		return ctrl.handlePVCSource(vmFileRestore, vmi)
	}

	// Handle Snapshot source - create a DV from snapshot, then proceed like PVC source
	if vmFileRestore.Spec.Source.Snapshot != nil {
		logger.Infof("Processing Snapshot source: snapshotName=%s", vmFileRestore.Spec.Source.Snapshot.Name)
		return ctrl.handleSnapshotSource(vmFileRestore, vmi)
	}

	// TODO: Handle Host source
	if vmFileRestore.Spec.Source.Host != nil {
		logger.Warningf("Host source not yet implemented")
		return nil
	}

	logger.Errorf("No valid source specified in VirtualMachineFileRestore spec")
	return nil
}

// addRestoreFinalizer adds the cleanup finalizer to the VirtualMachineFileRestore
func (ctrl *VMFileRestoreController) addRestoreFinalizer(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore) error {
	vmFileRestoreCopy := vmFileRestore.DeepCopy()
	vmFileRestoreCopy.Finalizers = append(vmFileRestoreCopy.Finalizers, fileRestoreFinalizer)

	_, err := ctrl.Client.VirtualMachineFileRestore(vmFileRestore.Namespace).Update(
		context.Background(),
		vmFileRestoreCopy,
		metav1.UpdateOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to add finalizer to VirtualMachineFileRestore: %w", err)
	}

	log.Log.Object(vmFileRestore).Info("Added cleanup finalizer to VirtualMachineFileRestore")
	return nil
}

// removeRestoreFinalizer removes the cleanup finalizer from the VirtualMachineFileRestore
func (ctrl *VMFileRestoreController) removeRestoreFinalizer(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore) error {
	vmFileRestoreCopy := vmFileRestore.DeepCopy()
	vmFileRestoreCopy.Finalizers = slices.DeleteFunc(vmFileRestoreCopy.Finalizers, func(f string) bool {
		return f == fileRestoreFinalizer
	})

	_, err := ctrl.Client.VirtualMachineFileRestore(vmFileRestore.Namespace).Update(
		context.Background(),
		vmFileRestoreCopy,
		metav1.UpdateOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from VirtualMachineFileRestore: %w", err)
	}

	log.Log.Object(vmFileRestore).Info("Removed cleanup finalizer from VirtualMachineFileRestore")
	return nil
}

// handleRestoreDeletion handles cleanup when a VirtualMachineFileRestore is being deleted
func (ctrl *VMFileRestoreController) handleRestoreDeletion(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore) error {
	logger := log.Log.Object(vmFileRestore)

	// Check if finalizer is present
	if !slices.Contains(vmFileRestore.Finalizers, fileRestoreFinalizer) {
		logger.Info("Finalizer not present, nothing to clean up")
		return nil
	}

	logger.Info("Handling VirtualMachineFileRestore deletion, cleaning up resources")

	// Get the VMI to unplug volume if still attached
	vmi, err := getVMIFromInformer(ctrl.VMIInformer, vmFileRestore.Namespace, vmFileRestore.Spec.VMIName)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return fmt.Errorf("failed to get VMI for cleanup: %w", err)
		}
		// VMI not found, nothing to unplug
		logger.Info("VMI not found, skipping volume unplug")
	} else if vmi.IsRunning() {
		// Unplug the volume if attached
		pvcName := ""
		if vmFileRestore.Spec.Source.PVC != nil {
			pvcName = vmFileRestore.Spec.Source.PVC.Name
		} else if vmFileRestore.Spec.Source.Snapshot != nil {
			pvcName = vmFileRestore.Spec.Source.Snapshot.Name
		}
		if pvcName != "" {
			volumeName := fmt.Sprintf("%s-restore", pvcName)

			// Check if volume is attached before trying to unplug
			if isVolumeAttached(vmi, volumeName, pvcName) {
				// For manual mode, we need to unmount first via guest command and wait for it
				if isManualMode(vmFileRestore.Spec.SourcePath) {
					unmountDone, err := ctrl.ensureRestoreUnmountComplete(vmFileRestore, vmi, volumeName, pvcName)
					if err != nil {
						logger.Reason(err).Warning("Failed to ensure unmount, proceeding with unplug anyway")
					} else if !unmountDone {
						// Unmount command created but not yet complete, requeue to wait
						logger.Info("Waiting for unmount command to complete before unplugging volume")
						return fmt.Errorf("waiting for unmount command to complete")
					}
				}

				logger.Infof("Unplugging volume %s from VMI %s", volumeName, vmi.Name)
				if err := unplugVolumeFromVM(ctrl.Client, vmi, volumeName); err != nil {
					logger.Reason(err).Errorf("Failed to unplug volume %s", volumeName)
					return err
				}
			}
		}
	}

	// Remove the finalizer to allow deletion to proceed
	return ctrl.removeRestoreFinalizer(vmFileRestore)
}

// unmountTimeout is the maximum time to wait for the unmount guest command to complete
// before proceeding with volume unplug regardless.
const unmountTimeout = 2 * time.Minute

// ensureRestoreUnmountComplete creates the unmount command if needed and checks if it's complete
// Returns (true, nil) if unmount is complete or not needed
// Returns (false, nil) if unmount command was created but not yet complete
// Returns (false, error) if there was an error
func (ctrl *VMFileRestoreController) ensureRestoreUnmountComplete(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore, vmi *v1.VirtualMachineInstance, volumeName, pvcName string) (bool, error) {
	logger := log.Log.Object(vmFileRestore)
	cmdName := fmt.Sprintf("%s-unmount", vmFileRestore.Name)

	// Check if the unmount command already exists
	existingCmd, err := ctrl.Client.VirtualMachineGuestCommand(vmFileRestore.Namespace).Get(
		context.Background(),
		cmdName,
		metav1.GetOptions{},
	)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return false, fmt.Errorf("failed to check for existing unmount command: %w", err)
		}
		// Command doesn't exist, create it
		if err := ctrl.createRestoreUnmountCommand(vmFileRestore, vmi, volumeName, pvcName); err != nil {
			return false, err
		}
		// Command just created, not complete yet
		return false, nil
	}

	// Command exists, check its status
	switch existingCmd.Status.Phase {
	case guestcommandv1alpha1.GuestCommandSucceeded:
		logger.Infof("Unmount command %s completed successfully", cmdName)
		return true, nil
	case guestcommandv1alpha1.GuestCommandFailed:
		logger.Warningf("Unmount command %s failed, proceeding with unplug anyway", cmdName)
		return true, nil // Proceed despite failure
	default:
		elapsed := time.Since(existingCmd.CreationTimestamp.Time)
		if elapsed > unmountTimeout {
			logger.Warningf("Unmount command %s has been in phase %q for %v (timeout %v), proceeding with unplug anyway",
				cmdName, existingCmd.Status.Phase, elapsed.Truncate(time.Second), unmountTimeout)
			return true, nil
		}
		logger.Infof("Unmount command %s is in phase %q for %v, waiting...", cmdName, existingCmd.Status.Phase, elapsed.Truncate(time.Second))
		return false, nil
	}
}

// createRestoreUnmountCommand creates a VirtualMachineGuestCommand to unmount the restore volume
func (ctrl *VMFileRestoreController) createRestoreUnmountCommand(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore, vmi *v1.VirtualMachineInstance, volumeName, pvcName string) error {
	logger := log.Log.Object(vmFileRestore)
	cmdName := fmt.Sprintf("%s-unmount", vmFileRestore.Name)

	// Check if the unmount command already exists
	_, err := ctrl.Client.VirtualMachineGuestCommand(vmFileRestore.Namespace).Get(
		context.Background(),
		cmdName,
		metav1.GetOptions{},
	)
	if err == nil {
		logger.Infof("Unmount command %s already exists", cmdName)
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for existing unmount command: %w", err)
	}

	// Construct the cleanup command using the appropriate guest script
	unmountCommand := fmt.Sprintf(`%s cleanup --mount-path %q`,
		getScriptPath(vmi), getMountPath(vmi, pvcName))

	guestCommand := &guestcommandv1alpha1.VirtualMachineGuestCommand{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmdName,
			Namespace: vmFileRestore.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "kubevirt-file-restore",
				"kubevirt.io/file-restore":     vmFileRestore.Name,
			},
			// DEBUG
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(vmFileRestore, filerestorev1alpha1.SchemeGroupVersion.WithKind("VirtualMachineFileRestore")),
			},
		},
		Spec: guestcommandv1alpha1.VirtualMachineGuestCommandSpec{
			VMIName: vmi.Name,
			Command: guestCommandShell(vmi, unmountCommand),
			Timeout: 30,
			Transport: &guestcommandv1alpha1.GuestTransport{
				VSOCK: &guestcommandv1alpha1.VSOCKReference{
					Name: vmi.Name,
				},
			},
		},
	}
	_, err = ctrl.Client.VirtualMachineGuestCommand(vmFileRestore.Namespace).Create(
		context.Background(),
		guestCommand,
		metav1.CreateOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to create unmount command: %w", err)
	}

	logger.Infof("Created unmount command %s", cmdName)
	return nil
}

// updateRestorePhase updates the Phase in VirtualMachineFileRestore status
func (ctrl *VMFileRestoreController) updateRestorePhase(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore, phase filerestorev1alpha1.FileOperationPhase) error {
	if vmFileRestore.Status != nil && vmFileRestore.Status.Phase == phase {
		return nil // No change needed
	}

	vmFileRestoreCopy := vmFileRestore.DeepCopy()
	if vmFileRestoreCopy.Status == nil {
		vmFileRestoreCopy.Status = &filerestorev1alpha1.VirtualMachineFileRestoreStatus{}
	}
	vmFileRestoreCopy.Status.Phase = phase

	_, err := ctrl.Client.VirtualMachineFileRestore(vmFileRestore.Namespace).UpdateStatus(
		context.Background(),
		vmFileRestoreCopy,
		metav1.UpdateOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to update VirtualMachineFileRestore status phase to %s: %w", phase, err)
	}

	log.Log.Object(vmFileRestore).Infof("Updated VirtualMachineFileRestore phase to %s", phase)
	return nil
}

// updateRestoreStatus updates the full status of VirtualMachineFileRestore
func (ctrl *VMFileRestoreController) updateRestoreStatus(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore, phase filerestorev1alpha1.FileOperationPhase, mountPath string) error {
	vmFileRestoreCopy := vmFileRestore.DeepCopy()
	if vmFileRestoreCopy.Status == nil {
		vmFileRestoreCopy.Status = &filerestorev1alpha1.VirtualMachineFileRestoreStatus{}
	}
	vmFileRestoreCopy.Status.Phase = phase
	vmFileRestoreCopy.Status.MountPath = mountPath

	_, err := ctrl.Client.VirtualMachineFileRestore(vmFileRestore.Namespace).UpdateStatus(
		context.Background(),
		vmFileRestoreCopy,
		metav1.UpdateOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to update VirtualMachineFileRestore status: %w", err)
	}

	log.Log.Object(vmFileRestore).Infof("Updated VirtualMachineFileRestore status: phase=%s, mountPath=%s", phase, mountPath)
	return nil
}

func (ctrl *VMFileRestoreController) handlePVCSource(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore, vmi *v1.VirtualMachineInstance) error {
	return ctrl.handlePVCRestore(vmFileRestore, vmi, vmFileRestore.Spec.Source.PVC.Name)
}

func (ctrl *VMFileRestoreController) handleSnapshotSource(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore, vmi *v1.VirtualMachineInstance) error {
	logger := log.Log.Object(vmFileRestore)
	snapshotName := vmFileRestore.Spec.Source.Snapshot.Name
	dvName := vmFileRestore.Spec.Source.Snapshot.Name

	dv, err := ctrl.ensureSnapshotDataVolume(vmFileRestore, dvName, snapshotName)
	if err != nil {
		return err
	}

	if dv.Status.Phase == cdiv1.Failed {
		logger.Errorf("DataVolume %s from snapshot %s failed", dvName, snapshotName)
		return ctrl.updateRestorePhase(vmFileRestore, filerestorev1alpha1.FileOperationPhaseFailed)
	}

	if dv.Status.Phase != cdiv1.Succeeded {
		logger.Infof("DataVolume %s is in phase %s, waiting for Succeeded", dvName, dv.Status.Phase)
		if vmFileRestore.Status.Phase != filerestorev1alpha1.FileOperationPhaseInProgress {
			return ctrl.updateRestorePhase(vmFileRestore, filerestorev1alpha1.FileOperationPhaseInProgress)
		}
		return nil
	}

	logger.Infof("DataVolume %s from snapshot %s succeeded, proceeding with PVC restore", dvName, snapshotName)
	return ctrl.handlePVCRestore(vmFileRestore, vmi, dvName)
}

// handlePVCRestore handles the common PVC-based restore logic.
// It hotplugs the PVC to the VMI, mounts it inside the guest, and triggers the restore.
// Used by both handlePVCSource (direct PVC) and handleSnapshotSource (DV-created PVC).
func (ctrl *VMFileRestoreController) handlePVCRestore(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore, vmi *v1.VirtualMachineInstance, pvcName string) error {
	logger := log.Log.Object(vmFileRestore)
	manualMode := isManualMode(vmFileRestore.Spec.SourcePath)

	// Check if PVC exists - for restore, the PVC MUST exist
	pvc, err := ctrl.Client.CoreV1().PersistentVolumeClaims(vmFileRestore.Namespace).Get(
		context.Background(),
		pvcName,
		metav1.GetOptions{},
	)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Errorf("Source PVC %s/%s not found - cannot restore from non-existent PVC", vmFileRestore.Namespace, pvcName)
			return fmt.Errorf("source PVC %s not found: cannot restore from non-existent PVC", pvcName)
		}
		return fmt.Errorf("failed to get PVC %s: %w", pvcName, err)
	}

	logger.Infof("Found source PVC %s, phase=%s, accessModes=%v", pvcName, pvc.Status.Phase, pvc.Spec.AccessModes)

	// Check if volume is already attached to VMI
	// Use a distinct volume name for the restore operation
	volumeName := fmt.Sprintf("%s-restore", pvcName)
	alreadyAttached := isVolumeAttached(vmi, volumeName, pvcName)

	// For manual mode, we don't check for completion - volume stays mounted until deletion
	// For automatic mode, check if restore has already completed
	if !manualMode {
		restoreComplete, guestCmdPhase, err := ctrl.getRestoreCompletionStatus(vmFileRestore)
		if err != nil {
			return err
		}
		if restoreComplete {
			// Update phase based on guest command result
			if guestCmdPhase == guestcommandv1alpha1.GuestCommandSucceeded {
				if err := ctrl.updateRestorePhase(vmFileRestore, filerestorev1alpha1.FileOperationPhaseSucceeded); err != nil {
					return err
				}
			} else if guestCmdPhase == guestcommandv1alpha1.GuestCommandFailed {
				if err := ctrl.updateRestorePhase(vmFileRestore, filerestorev1alpha1.FileOperationPhaseFailed); err != nil {
					return err
				}
			}

			if alreadyAttached {
				// Volume still attached after restore completed - unplug it
				logger.Infof("Restore already complete for VMFileRestore %s, unplugging volume %s", vmFileRestore.Name, volumeName)
				if err := unplugVolumeFromVM(ctrl.Client, vmi, volumeName); err != nil {
					return err
				}
			} else {
				logger.Infof("Restore already complete for VMFileRestore %s, volume already unplugged", vmFileRestore.Name)
			}
			return nil
		}
	}

	// For manual mode with VolumeReady phase, just ensure volume stays attached
	if manualMode && vmFileRestore.Status.Phase == filerestorev1alpha1.FileOperationPhaseVolumeReady {
		if alreadyAttached {
			logger.V(3).Infof("Manual mode: volume %s is attached and ready for manual restore operations", volumeName)
		} else {
			// Volume was detached unexpectedly, need to re-attach
			logger.Warningf("Manual mode: volume %s was detached, re-attaching", volumeName)
		}
		// Continue to ensure volume is attached
	}

	// Set phase to InProgress once we start working (if not already VolumeReady in manual mode)
	if vmFileRestore.Status.Phase != filerestorev1alpha1.FileOperationPhaseInProgress &&
		vmFileRestore.Status.Phase != filerestorev1alpha1.FileOperationPhaseVolumeReady {
		if err := ctrl.updateRestorePhase(vmFileRestore, filerestorev1alpha1.FileOperationPhaseInProgress); err != nil {
			return err
		}
	}

	if alreadyAttached {
		logger.Infof("PVC %s is already attached to VMI %s as volume %s", pvcName, vmi.Name, volumeName)
	} else {
		logger.Infof("Initiating declarative hotplug of PVC %s to VMI %s as volume %s (read-only)", pvcName, vmi.Name, volumeName)
		// Mount read-only to protect the source PVC from being modified by the guest
		err = hotplugPVCToVM(ctrl.Client, vmi, pvcName, volumeName, false)
		if err != nil {
			logger.Reason(err).Errorf("Failed to hotplug PVC %s to VMI %s", pvcName, vmi.Name)
			return fmt.Errorf("failed to hotplug PVC %s to VMI: %w", pvcName, err)
		}
		logger.Infof("Successfully requested declarative hotplug of PVC %s to VMI %s", pvcName, vmi.Name)
	}

	// Check if volume is ready (mounted and has a target path in the VM)
	volumeReady, targetDevice := isVolumeReady(vmi, volumeName)
	if !volumeReady {
		logger.Infof("Volume %s is not yet ready in VMI %s, waiting for hotplug to complete", volumeName, vmi.Name)
		// Return nil to allow requeue - the controller will retry
		return nil
	}

	logger.Infof("Volume %s is ready in VMI %s with target device %s", volumeName, vmi.Name, targetDevice)

	// Create a VirtualMachineGuestCommand to mount the restore volume and restore files in the guest
	// Pass volumeName as the serial to find the device inside the guest (more reliable than targetDevice)
	err = ctrl.createRestoreMountCommand(vmFileRestore, vmi, volumeName, pvcName)
	if err != nil {
		logger.Reason(err).Errorf("Failed to create guest command for restoring from source volume")
		return fmt.Errorf("failed to create guest command for restoring from source volume: %w", err)
	}

	// For manual mode, check if mount command completed and set VolumeReady phase
	if manualMode {
		mountComplete, mountPhase, err := ctrl.getRestoreCompletionStatus(vmFileRestore)
		if err != nil {
			return err
		}
		if mountComplete {
			if mountPhase == guestcommandv1alpha1.GuestCommandSucceeded {
				// Volume is mounted and ready for manual operations
				if vmFileRestore.Status.Phase != filerestorev1alpha1.FileOperationPhaseVolumeReady {
					logger.Infof("Manual mode: volume mounted at %s, setting phase to VolumeReady", getMountPath(vmi, pvcName))
					if err := ctrl.updateRestoreStatus(vmFileRestore, filerestorev1alpha1.FileOperationPhaseVolumeReady, getMountPath(vmi, pvcName)); err != nil {
						return err
					}
				}
			} else if mountPhase == guestcommandv1alpha1.GuestCommandFailed {
				if err := ctrl.updateRestorePhase(vmFileRestore, filerestorev1alpha1.FileOperationPhaseFailed); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// getRestoreCompletionStatus checks if the restore guest command has completed (succeeded or failed)
// Returns: (isComplete, guestCommandPhase, error)
func (ctrl *VMFileRestoreController) getRestoreCompletionStatus(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore) (bool, guestcommandv1alpha1.GuestCommandPhase, error) {
	existingCmd, err := ctrl.Client.VirtualMachineGuestCommand(vmFileRestore.Namespace).Get(
		context.Background(),
		guestCommandName(vmFileRestore.Name),
		metav1.GetOptions{},
	)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// Guest command doesn't exist yet, restore not started
			return false, "", nil
		}
		return false, "", fmt.Errorf("failed to check guest command status: %w", err)
	}

	isComplete := existingCmd.Status.Phase == guestcommandv1alpha1.GuestCommandSucceeded || existingCmd.Status.Phase == guestcommandv1alpha1.GuestCommandFailed
	return isComplete, existingCmd.Status.Phase, nil
}

// createRestoreMountCommand creates a VirtualMachineGuestCommand to mount the source volume and restore files
// For manual mode (empty sourcePath), the command only mounts the volume without rsync/unmount
func (ctrl *VMFileRestoreController) createRestoreMountCommand(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore, vmi *v1.VirtualMachineInstance, volumeName, pvcName string) error {
	logger := log.Log.Object(vmFileRestore)
	cmdName := guestCommandName(vmFileRestore.Name)
	manualMode := isManualMode(vmFileRestore.Spec.SourcePath)

	// Check if the guest command already exists
	existingCmd, err := ctrl.Client.VirtualMachineGuestCommand(vmFileRestore.Namespace).Get(
		context.Background(),
		cmdName,
		metav1.GetOptions{},
	)
	if err == nil {
		// Guest command already exists, nothing more to do here
		// The unplug is handled by the main sync loop when restore is complete
		logger.Infof("Guest command %s already exists with phase %s", cmdName, existingCmd.Status.Phase)
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for existing guest command: %w", err)
	}

	// Construct the restore command using the filerestore.sh script on the guest.
	// The script finds the device by serial (volumeName) since the device name
	// may differ from the VMI target (e.g., vdc vs vde).
	scriptPath := getScriptPath(vmi)
	var mountCommand string
	if manualMode {
		// Manual mode: mount read-only only, user will perform restore manually
		mountCommand = fmt.Sprintf(`%s restore --serial %q --mount-path %q`,
			scriptPath, volumeName, getMountPath(vmi, pvcName))
	} else {
		// Automatic mode: mount read-only, rsync/robocopy files FROM source TO VMI, sync, unmount
		mountCommand = fmt.Sprintf(`%s restore --serial %q --mount-path %q --source-path %q`,
			scriptPath, volumeName, getMountPath(vmi, pvcName), vmFileRestore.Spec.SourcePath)
	}

	guestCommand := &guestcommandv1alpha1.VirtualMachineGuestCommand{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmdName,
			Namespace: vmFileRestore.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "kubevirt-file-restore",
				"kubevirt.io/file-restore":     vmFileRestore.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(vmFileRestore, filerestorev1alpha1.SchemeGroupVersion.WithKind("VirtualMachineFileRestore")),
			},
		},

		Spec: guestcommandv1alpha1.VirtualMachineGuestCommandSpec{
			VMIName: vmi.Name,
			Command: guestCommandShell(vmi, mountCommand),
			Timeout: 60,
			Transport: &guestcommandv1alpha1.GuestTransport{
				VSOCK: &guestcommandv1alpha1.VSOCKReference{
					Name: vmi.Name,
				},
			},
		},
	}
	createdCmd, err := ctrl.Client.VirtualMachineGuestCommand(vmFileRestore.Namespace).Create(
		context.Background(),
		guestCommand,
		metav1.CreateOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to create guest command: %w", err)
	}

	if manualMode {
		logger.Infof("Successfully created VirtualMachineGuestCommand %s to mount volume at %s for manual restore", createdCmd.Name, getMountPath(vmi, pvcName))
	} else {
		logger.Infof("Successfully created VirtualMachineGuestCommand %s to restore files using serial %s", createdCmd.Name, volumeName)
	}
	return nil
}

// ensureSnapshotDataVolume creates a DataVolume from a VolumeSnapshot if it doesn't already exist.
// An empty Storage spec is required for CDI webhook validation; CDI derives the actual size from the snapshot's restoreSize.
// The DV is owned by the VMFileRestore so it is garbage-collected on deletion.
func (ctrl *VMFileRestoreController) ensureSnapshotDataVolume(vmFileRestore *filerestorev1alpha1.VirtualMachineFileRestore, dvName, snapshotName string) (*cdiv1.DataVolume, error) {
	key := fmt.Sprintf("%s/%s", vmFileRestore.Namespace, dvName)
	obj, exists, err := ctrl.DataVolumeInformer.GetStore().GetByKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get DataVolume from store: %w", err)
	}
	if exists {
		return obj.(*cdiv1.DataVolume), nil
	}

	dv := &cdiv1.DataVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dvName,
			Namespace: vmFileRestore.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "kubevirt-file-restore",
				"kubevirt.io/file-restore":     vmFileRestore.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(vmFileRestore, filerestorev1alpha1.SchemeGroupVersion.WithKind("VirtualMachineFileRestore")),
			},
		},
		Spec: cdiv1.DataVolumeSpec{
			Source: &cdiv1.DataVolumeSource{
				Snapshot: &cdiv1.DataVolumeSourceSnapshot{
					Name:      snapshotName,
					Namespace: vmFileRestore.Namespace,
				},
			},
			Storage: &cdiv1.StorageSpec{
				AccessModes: []k8sv1.PersistentVolumeAccessMode{
					k8sv1.ReadWriteOnce,
				},
			},
		},
	}

	createdDV, err := ctrl.Client.CdiClient().CdiV1beta1().DataVolumes(vmFileRestore.Namespace).Create(
		context.Background(),
		dv,
		metav1.CreateOptions{},
	)
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			existingDV, getErr := ctrl.Client.CdiClient().CdiV1beta1().DataVolumes(vmFileRestore.Namespace).Get(
				context.Background(), dvName, metav1.GetOptions{})
			if getErr != nil {
				return nil, fmt.Errorf("failed to get existing DataVolume: %w", getErr)
			}
			return existingDV, nil
		}
		return nil, fmt.Errorf("failed to create DataVolume from snapshot: %w", err)
	}

	log.Log.Object(vmFileRestore).Infof("Created DataVolume %s from snapshot %s", dvName, snapshotName)
	return createdDV, nil
}

func (ctrl *VMFileRestoreController) handleVMFileRestore(obj interface{}) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}

	if vmFileRestore, ok := obj.(*filerestorev1alpha1.VirtualMachineFileRestore); ok {
		key, err := cache.MetaNamespaceKeyFunc(vmFileRestore)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf(failedKeyFromObjectFmt, vmFileRestore, err))
			return
		}
		ctrl.vmFileRestoreQueue.Add(key)
		log.Log.Object(vmFileRestore).Infof("Enqueued VirtualMachineFileRestore for sync: %s", key)
		return
	}

	utilruntime.HandleError(fmt.Errorf(unexpectedResourceFmt, obj))
}

func (ctrl *VMFileRestoreController) handleVM(obj interface{}) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}

	// TODO: Handle VM changes that affect file restores
	if vm, ok := obj.(*v1.VirtualMachine); ok {
		log.Log.Object(vm).V(3).Infof("VM %s/%s changed, checking for related file restores", vm.Namespace, vm.Name)
	}
}

func (ctrl *VMFileRestoreController) handleVMI(obj interface{}) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}

	vmi, ok := obj.(*v1.VirtualMachineInstance)
	if !ok {
		utilruntime.HandleError(fmt.Errorf(unexpectedResourceFmt, obj))
		return
	}

	// Find all VMFileRestores that reference this VMI and enqueue them
	ctrl.enqueueVMFileRestoresForVMI(vmi)
}

func (ctrl *VMFileRestoreController) handleGuestCommand(obj interface{}) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}

	guestCmd, ok := obj.(*guestcommandv1alpha1.VirtualMachineGuestCommand)
	if !ok {
		utilruntime.HandleError(fmt.Errorf(unexpectedResourceFmt, obj))
		return
	}

	// Check if this guest command is managed by our controller
	if guestCmd.Labels == nil {
		return
	}
	vmFileRestoreName, ok := guestCmd.Labels["kubevirt.io/file-restore"]
	if !ok {
		return
	}

	// Enqueue the related VMFileRestore
	key := fmt.Sprintf("%s/%s", guestCmd.Namespace, vmFileRestoreName)
	log.Log.V(3).Infof("GuestCommand %s/%s changed (phase=%s), enqueuing related VMFileRestore %s",
		guestCmd.Namespace, guestCmd.Name, guestCmd.Status.Phase, key)
	ctrl.vmFileRestoreQueue.Add(key)
}

func (ctrl *VMFileRestoreController) handleDataVolume(obj interface{}) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}

	dv, ok := obj.(*cdiv1.DataVolume)
	if !ok {
		utilruntime.HandleError(fmt.Errorf(unexpectedResourceFmt, obj))
		return
	}

	if dv.Labels == nil {
		return
	}
	vmFileRestoreName, ok := dv.Labels["kubevirt.io/file-restore"]
	if !ok {
		return
	}

	key := fmt.Sprintf("%s/%s", dv.Namespace, vmFileRestoreName)
	log.Log.V(3).Infof("DataVolume %s/%s changed (phase=%s), enqueuing related VMFileRestore %s",
		dv.Namespace, dv.Name, dv.Status.Phase, key)
	ctrl.vmFileRestoreQueue.Add(key)
}

// enqueueVMFileRestoresForVMI finds all VMFileRestores that reference the given VMI and enqueues them
func (ctrl *VMFileRestoreController) enqueueVMFileRestoresForVMI(vmi *v1.VirtualMachineInstance) {
	// Get all VMFileRestores from the store
	vmFileRestores := ctrl.VMFileRestoreInformer.GetStore().List()

	for _, obj := range vmFileRestores {
		vmFileRestore, ok := obj.(*filerestorev1alpha1.VirtualMachineFileRestore)
		if !ok {
			continue
		}

		// Check if this VMFileRestore references the VMI
		if vmFileRestore.Namespace == vmi.Namespace && vmFileRestore.Spec.VMIName == vmi.Name {
			key, err := cache.MetaNamespaceKeyFunc(vmFileRestore)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf(failedKeyFromObjectFmt, vmFileRestore, err))
				continue
			}
			log.Log.Object(vmi).V(3).Infof("VMI %s/%s changed, enqueuing related VMFileRestore %s", vmi.Namespace, vmi.Name, key)
			ctrl.vmFileRestoreQueue.Add(key)
		}
	}
}
