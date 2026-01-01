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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FileOperationPhase represents the phase of a file restore operation
// +kubebuilder:validation:Enum=Pending;InProgress;VolumeReady;Succeeded;Failed
type FileOperationPhase string

const (
	// FileOperationPhasePending indicates the operation has not started yet
	FileOperationPhasePending FileOperationPhase = "Pending"
	// FileOperationPhaseInProgress indicates the operation is currently running
	FileOperationPhaseInProgress FileOperationPhase = "InProgress"
	// FileOperationPhaseVolumeReady indicates the volume is mounted and ready for manual operation
	// This phase is used when sourcePath is empty, allowing users to manually perform restore
	FileOperationPhaseVolumeReady FileOperationPhase = "VolumeReady"
	// FileOperationPhaseSucceeded indicates the operation completed successfully
	FileOperationPhaseSucceeded FileOperationPhase = "Succeeded"
	// FileOperationPhaseFailed indicates the operation failed
	FileOperationPhaseFailed FileOperationPhase = "Failed"
)

// VirtualMachineFileRestore defines the operation of restoring files to a VM
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=vmfr;vmfilerestore,scope=Namespaced
type VirtualMachineFileRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec VirtualMachineFileRestoreSpec `json:"spec"`

	// +optional
	Status *VirtualMachineFileRestoreStatus `json:"status,omitempty"`
}

// VirtualMachineFileRestoreSpec is the spec for a VirtualMachineFileRestore resource
type VirtualMachineFileRestoreSpec struct {
	// VMIName specifies the target VirtualMachineInstance name
	// The VMI must be running for the restore operation to proceed
	// +kubebuilder:validation:Required
	VMIName string `json:"vmiName"`

	// Source specifies where to restore files from
	// Exactly one of PVC, Snapshot, or Host must be specified
	// +kubebuilder:validation:Required
	Source FileRestoreSource `json:"source"`

	// SourcePath specifies the path on the source to restore from
	// If empty, the restore volume will be mounted at /backup for manual operation,
	// and will remain mounted until the VirtualMachineFileRestore is deleted.
	// +optional
	SourcePath string `json:"sourcePath,omitempty"`

	// TargetPath specifies the path on the target VMI to restore to
	// If not specified, defaults to the same path as SourcePath
	// +optional
	TargetPath string `json:"targetPath,omitempty"`
}

// VirtualMachineFileRestoreStatus is the status for a VirtualMachineFileRestore resource
type VirtualMachineFileRestoreStatus struct {
	// Phase represents the current phase of the file restore operation
	// +optional
	Phase FileOperationPhase `json:"phase,omitempty"`

	// MountPath is the path where the restore volume is mounted inside the VM guest
	// This is populated when the volume is ready and is useful for manual restore operations
	// +optional
	MountPath string `json:"mountPath,omitempty"`
}

// FileRestoreSource represents the source for file restore operations
// Exactly one of PVC, Snapshot, or Host must be specified
type FileRestoreSource struct {
	// PVC specifies a PersistentVolumeClaim in the same namespace as the source
	// +optional
	PVC *FileRestoreSourcePVC `json:"pvc,omitempty"`

	// Snapshot specifies a VolumeSnapshot in the same namespace as the source
	// +optional
	Snapshot *FileRestoreSourceSnapshot `json:"snapshot,omitempty"`

	// Host specifies an SSH-able host as the source
	// +optional
	Host *FileRestoreSourceHost `json:"host,omitempty"`
}

// FileRestoreSourcePVC provides the parameters to restore from a PVC
type FileRestoreSourcePVC struct {
	// Name of the PersistentVolumeClaim in the same namespace
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// FileRestoreSourceSnapshot provides the parameters to restore from a VolumeSnapshot
type FileRestoreSourceSnapshot struct {
	// Name of the VolumeSnapshot in the same namespace
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// FileRestoreSourceHost provides the parameters to restore from an SSH-able host
type FileRestoreSourceHost struct {
	// Host specifies the SSH host address (hostname or IP)
	// +kubebuilder:validation:Required
	Host string `json:"host"`

	// Port specifies the SSH port (defaults to 22 if not specified)
	// +optional
	//Port int32 `json:"port,omitempty"`

	// User specifies the SSH user (defaults to "root" if not specified)
	// +optional
	//User string `json:"user,omitempty"`

	// SSHKeySecret is a reference to a secret containing the SSH private key
	// The secret should contain a key named "ssh-privatekey" (or the key specified in SSHKeySecretKey)
	// +optional
	//SSHKeySecret *corev1.LocalObjectReference `json:"sshKeySecret,omitempty"`

	// SSHKeySecretKey specifies the key name in the SSHKeySecret
	// Defaults to "ssh-privatekey" if not specified
	// +optional
	//SSHKeySecretKey string `json:"sshKeySecretKey,omitempty"`
}

// VirtualMachineFileRestoreList is a list of VirtualMachineFileRestore resources
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type VirtualMachineFileRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []VirtualMachineFileRestore `json:"items"`
}
