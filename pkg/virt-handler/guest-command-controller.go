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

package virthandler

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/ssh"
	k8sv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	v1 "kubevirt.io/api/core/v1"
	guestcommandv1alpha1 "kubevirt.io/api/guestcommand/v1alpha1"
	"kubevirt.io/client-go/kubecli"
	kvcorev1 "kubevirt.io/client-go/kubevirt/typed/core/v1"
	"kubevirt.io/client-go/log"

	"kubevirt.io/kubevirt/pkg/pointer"
)

const (
	// SuccessfulGuestCommandExecutionReason is added when command execution succeeds
	SuccessfulGuestCommandExecutionReason = "SuccessfulExecution"
	// FailedGuestCommandExecutionReason is added when command execution fails
	FailedGuestCommandExecutionReason = "FailedExecution"
	// VMINotRunningReason is added when target VMI is not running
	VMINotRunningReason = "VMINotRunning"
	// VSOCKNotAvailableReason is added when VSOCK is not available on the VMI
	VSOCKNotAvailableReason = "VSOCKNotAvailable"
	// SSHConnectionFailedReason is added when SSH connection fails
	SSHConnectionFailedReason = "SSHConnectionFailed"
	// VSOCKConfigNotFoundReason is added when the referenced VSOCKConfig is not found
	VSOCKConfigNotFoundReason = "VSOCKConfigNotFound"
	// TransportNotConfiguredReason is added when Transport is not configured
	TransportNotConfiguredReason = "TransportNotConfigured"
)

// GuestCommandController monitors VirtualMachineGuestCommand objects and executes
// commands in guest VMs via SSH over VSOCK
type GuestCommandController struct {
	clientset       kubecli.KubevirtClient
	vmiInformer     cache.SharedIndexInformer
	commandInformer cache.SharedIndexInformer
	recorder        record.EventRecorder
	queue           workqueue.RateLimitingInterface
	hostName        string
}

// NewGuestCommandController creates a new guest command controller
func NewGuestCommandController(
	clientset kubecli.KubevirtClient,
	vmiInformer cache.SharedIndexInformer,
	commandInformer cache.SharedIndexInformer,
	recorder record.EventRecorder,
) *GuestCommandController {
	hostName, _ := os.LookupEnv("NODE_NAME")
	c := &GuestCommandController{
		clientset:       clientset,
		vmiInformer:     vmiInformer,
		commandInformer: commandInformer,
		recorder:        recorder,
		queue:           workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "virt-handler-guest-command"),
		hostName:        hostName,
	}

	_, err := c.commandInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addGuestCommand,
		UpdateFunc: c.updateGuestCommand,
		DeleteFunc: c.deleteGuestCommand,
	})
	if err != nil {
		panic(err)
	}

	return c
}

func (c *GuestCommandController) addGuestCommand(obj interface{}) {
	command := obj.(*guestcommandv1alpha1.VirtualMachineGuestCommand)
	log.Log.V(3).Infof("GuestCommand add event: %s/%s (phase=%s, generation=%d)",
		command.Namespace, command.Name, command.Status.Phase, command.Generation)
	// Don't enqueue if already processed successfully
	if c.shouldSkipExecution(command) {
		log.Log.V(3).Infof("Skipping GuestCommand %s/%s - shouldSkipExecution returned true", command.Namespace, command.Name)
		return
	}
	log.Log.V(3).Infof("Enqueueing GuestCommand %s/%s for processing", command.Namespace, command.Name)
	c.enqueueGuestCommand(obj)
}

func (c *GuestCommandController) updateGuestCommand(old, cur interface{}) {
	oldCommand := old.(*guestcommandv1alpha1.VirtualMachineGuestCommand)
	curCommand := cur.(*guestcommandv1alpha1.VirtualMachineGuestCommand)

	// Don't enqueue if already processed successfully
	if c.shouldSkipExecution(curCommand) {
		return
	}

	// Always enqueue commands with no status (not yet processed) to handle resync
	// This ensures commands that were missed on initial add (e.g., due to timing issues
	// where VMI wasn't detected on this node yet) are eventually processed
	if curCommand.Status.Phase == "" {
		log.Log.V(3).Infof("GuestCommand %s/%s has no status, enqueueing for processing (resync recovery)",
			curCommand.Namespace, curCommand.Name)
		c.enqueueGuestCommand(cur)
		return
	}

	// Only enqueue if the spec changed (generation changed), not on status updates
	if oldCommand.Generation != curCommand.Generation {
		log.Log.V(3).Infof("GuestCommand %s/%s generation changed (%d -> %d), enqueueing",
			curCommand.Namespace, curCommand.Name, oldCommand.Generation, curCommand.Generation)
		c.enqueueGuestCommand(cur)
	}
}

func (c *GuestCommandController) deleteGuestCommand(obj interface{}) {
	// Cleanup if needed
}

func (c *GuestCommandController) enqueueGuestCommand(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		return
	}
	c.queue.Add(key)
}

// Run starts the controller
func (c *GuestCommandController) Run(ctx context.Context, threadiness int) {
	defer c.queue.ShutDown()
	log.Log.Info("Starting guest command controller")

	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, ctx.Done())
	}

	<-ctx.Done()
	log.Log.Info("Stopping guest command controller")
}

func (c *GuestCommandController) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *GuestCommandController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.execute(key.(string))
	c.handleErr(err, key)
	return true
}

func (c *GuestCommandController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < 5 {
		log.Log.Reason(err).Infof("Error syncing guest command %v: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	log.Log.Reason(err).Infof("Dropping guest command %v out of the queue: %v", key, err)
	c.queue.Forget(key)
}

func (c *GuestCommandController) execute(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	logger := log.Log.With("controller", "guest-command", "namespace", namespace, "name", name)

	// Get the GuestCommand CR
	commandObj, exists, err := c.commandInformer.GetIndexer().GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		logger.V(3).Info("Guest command does not exist anymore")
		return nil
	}

	command := commandObj.(*guestcommandv1alpha1.VirtualMachineGuestCommand).DeepCopy()

	// Skip if already succeeded (but not if Running, as we may need to finish it)
	if c.shouldSkipExecutionCompleted(command) {
		logger.V(3).Info("Skipping execution - already completed")
		return nil
	}

	// Get the target VMI
	vmiObj, exists, err := c.vmiInformer.GetIndexer().GetByKey(namespace + "/" + command.Spec.VMIName)
	if err != nil {
		return err
	}
	if !exists {
		_, err = c.updateCommandStatus(command, guestcommandv1alpha1.GuestCommandFailed, 1, "", "",
			fmt.Sprintf("VirtualMachineInstance %s not found", command.Spec.VMIName),
			VMINotRunningReason)
		return err
	}

	vmi := vmiObj.(*v1.VirtualMachineInstance)

	// Check if the VMI is on this node
	if !c.isVMIOnThisNode(vmi) {
		// If the VMI's node name is not set yet (still scheduling), don't skip silently
		// as this could be a timing issue. Return an error to trigger requeue.
		if vmi.Status.NodeName == "" {
			logger.V(3).Infof("VMI %s has no NodeName set yet, will requeue", vmi.Name)
			return fmt.Errorf("VMI %s NodeName not set, will retry", vmi.Name)
		}
		logger.V(3).Infof("VMI %s is not on this node (%s, VMI is on %s), skipping execution", vmi.Name, c.hostName, vmi.Status.NodeName)
		return nil
	}

	// Validate VMI is running
	if vmi.Status.Phase != v1.Running {
		_, err = c.updateCommandStatus(command, guestcommandv1alpha1.GuestCommandPending, 0, "", "",
			fmt.Sprintf("VirtualMachineInstance %s is not running (phase: %s)", vmi.Name, vmi.Status.Phase),
			VMINotRunningReason)
		return err
	}

	// Check VSOCK availability
	if vmi.Status.VSOCKCID == nil {
		_, err = c.updateCommandStatus(command, guestcommandv1alpha1.GuestCommandPending, 0, "", "",
			"VSOCK is not available on the VirtualMachineInstance",
			VSOCKNotAvailableReason)
		return err
	}

	// Get VSOCKConfig from Transport reference
	var vsockConfig *guestcommandv1alpha1.VSOCKConfig
	if command.Spec.Transport != nil && command.Spec.Transport.VSOCK != nil {
		vsockConfigName := command.Spec.Transport.VSOCK.Name
		vsockConfig, err = c.clientset.VSOCKConfig(namespace).Get(context.Background(), vsockConfigName, metav1.GetOptions{})
		if err != nil {
			_, err = c.updateCommandStatus(command, guestcommandv1alpha1.GuestCommandFailed, 1, "", "",
				fmt.Sprintf("VSOCKConfig %s not found: %v", vsockConfigName, err),
				VSOCKConfigNotFoundReason)
			return err
		}
	} else {
		_, err = c.updateCommandStatus(command, guestcommandv1alpha1.GuestCommandFailed, 1, "", "",
			"Transport with VSOCK reference is required",
			TransportNotConfiguredReason)
		return err
	}

	// Check if already running or completed to prevent concurrent execution
	// Re-fetch from cache to get latest status
	commandObj, exists, err = c.commandInformer.GetIndexer().GetByKey(key)
	if err != nil || !exists {
		return err
	}
	command = commandObj.(*guestcommandv1alpha1.VirtualMachineGuestCommand).DeepCopy()

	// If already running or completed, skip
	if command.Status.Phase == guestcommandv1alpha1.GuestCommandRunning ||
		command.Status.Phase == guestcommandv1alpha1.GuestCommandSucceeded ||
		command.Status.Phase == guestcommandv1alpha1.GuestCommandFailed {
		logger.V(3).Infof("Command already in phase %s, skipping execution", command.Status.Phase)
		return nil
	}

	// Update to Running phase and use the returned updated object
	command, err = c.updateCommandStatus(command, guestcommandv1alpha1.GuestCommandRunning, 0, "", "",
		"Executing command in guest",
		"Executing")
	if err != nil {
		return err
	}

	// Execute the command
	timeout := command.Spec.Timeout
	if timeout == 0 {
		timeout = 30 // default timeout
	}

	var commandPath string
	var args []string
	if len(command.Spec.Command) > 0 {
		commandPath = command.Spec.Command[0]
		if len(command.Spec.Command) > 1 {
			args = command.Spec.Command[1:]
		}
	}

	var exitCode int
	var stdout, stderr string
	var execErr error

	// Execute via VSOCK
	vsockLogger := log.Log.With("controller", "guest-command", "namespace", command.Namespace, "name", command.Name, "method", "vsock")
	exitCode, stdout, stderr, execErr = c.executeViaVSOCK(vmi, &vsockConfig.Spec, commandPath, args, timeout, vsockLogger)

	// Update status based on result
	var phase guestcommandv1alpha1.GuestCommandPhase
	var reason string
	var message string

	if execErr != nil {
		phase = guestcommandv1alpha1.GuestCommandFailed
		reason = FailedGuestCommandExecutionReason
		message = fmt.Sprintf("Command execution failed: %v", execErr)
	} else if exitCode == 0 {
		phase = guestcommandv1alpha1.GuestCommandSucceeded
		reason = SuccessfulGuestCommandExecutionReason
		message = "Command completed successfully"
	} else {
		phase = guestcommandv1alpha1.GuestCommandFailed
		reason = FailedGuestCommandExecutionReason
		message = fmt.Sprintf("Command exited with code %d", exitCode)
	}

	command, err = c.updateCommandStatus(command, phase, int32(exitCode), stdout, stderr, message, reason)
	if err != nil {
		return err
	}

	// Record event
	eventType := k8sv1.EventTypeNormal
	if phase == guestcommandv1alpha1.GuestCommandFailed {
		eventType = k8sv1.EventTypeWarning
	}
	c.recorder.Eventf(command, eventType, reason, message)

	return nil
}

func (c *GuestCommandController) shouldSkipExecution(command *guestcommandv1alpha1.VirtualMachineGuestCommand) bool {
	// Always skip if currently running to prevent concurrent execution
	if command.Status.Phase == guestcommandv1alpha1.GuestCommandRunning {
		return true
	}

	return c.shouldSkipExecutionCompleted(command)
}

func (c *GuestCommandController) shouldSkipExecutionCompleted(command *guestcommandv1alpha1.VirtualMachineGuestCommand) bool {
	// Check if already succeeded or failed based on run policy
	if command.Status.Phase == guestcommandv1alpha1.GuestCommandSucceeded ||
		command.Status.Phase == guestcommandv1alpha1.GuestCommandFailed {

		// For RunOnce, always skip if already completed
		if command.Spec.RunPolicy == guestcommandv1alpha1.GuestCommandRunOnce {
			return true
		}

		// For RunOnChange, skip if generation hasn't changed
		if command.Spec.RunPolicy == guestcommandv1alpha1.GuestCommandRunOnChange {
			if command.Status.ObservedGeneration == command.Generation {
				return true
			}
		}
	}

	return false
}

func (c *GuestCommandController) isVMIOnThisNode(vmi *v1.VirtualMachineInstance) bool {
	// Check node label first (similar to main VMI controller)
	nodeName, ok := vmi.Labels[v1.NodeNameLabel]
	if ok && nodeName != "" && nodeName == c.hostName {
		return true
	}

	// Check status node name
	if vmi.Status.NodeName != "" && vmi.Status.NodeName == c.hostName {
		return true
	}

	return false
}

// executeViaVSOCK executes a command in the guest VM via SSH over VSOCK
func (c *GuestCommandController) executeViaVSOCK(
	vmi *v1.VirtualMachineInstance,
	vsockConfigSpec *guestcommandv1alpha1.VSOCKConfigSpec,
	commandPath string,
	args []string,
	timeout int32,
	logger *log.FilteredLogger,
) (exitCode int, stdout, stderr string, err error) {
	// Set defaults
	port := vsockConfigSpec.Port
	if port == 0 {
		port = 22 // default SSH port
	}

	user := vsockConfigSpec.User
	if user == "" {
		user = "root"
	}

	useTLS := vsockConfigSpec.UseTLS

	// Get SSH private key
	authMethods, err := c.getSSHAuthMethods(vmi, vsockConfigSpec, logger)
	if err != nil {
		return 1, "", "", fmt.Errorf("failed to get SSH authentication: %w", err)
	}

	// Create VSOCK connection
	vsockStream, err := c.clientset.VirtualMachineInstance(vmi.Namespace).VSOCK(
		vmi.Name,
		&v1.VSOCKOptions{
			TargetPort: port,
			UseTLS:     pointer.P(useTLS),
		},
	)
	if err != nil {
		return 1, "", "", fmt.Errorf("failed to create VSOCK connection: %w", err)
	}

	// Create pipe for SSH connection
	cliConn, svrConn := net.Pipe()
	defer func() {
		_ = cliConn.Close()
		_ = svrConn.Close()
	}()

	// Stream VSOCK data through the pipe
	streamErrCh := make(chan error, 1)
	go func() {
		streamErrCh <- vsockStream.Stream(kvcorev1.StreamOptions{In: svrConn, Out: svrConn})
	}()

	// Configure SSH client
	sshConfig := &ssh.ClientConfig{
		User:            user,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // VSOCK is already secure
		Timeout:         time.Duration(timeout) * time.Second,
	}

	// Establish SSH connection
	sshConn, chans, reqs, err := ssh.NewClientConn(cliConn, "", sshConfig)
	if err != nil {
		return 1, "", "", fmt.Errorf("failed to establish SSH connection: %w", err)
	}
	defer sshConn.Close()

	client := ssh.NewClient(sshConn, chans, reqs)
	defer client.Close()

	// Create SSH session
	session, err := client.NewSession()
	if err != nil {
		return 1, "", "", fmt.Errorf("failed to create SSH session: %w", err)
	}
	defer session.Close()

	// Set up stdout and stderr capture
	var stdoutBuf, stderrBuf []byte
	session.Stdout = &stdoutCapture{data: &stdoutBuf}
	session.Stderr = &stderrCapture{data: &stderrBuf}

	// Build command string
	cmdStr := commandPath
	for _, arg := range args {
		// Simple escaping for shell safety
		cmdStr += " " + escapeShellArg(arg)
	}

	// Execute command with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- session.Run(cmdStr)
	}()

	select {
	case <-ctx.Done():
		session.Close()
		return 1, string(stdoutBuf), string(stderrBuf), fmt.Errorf("command execution timeout after %d seconds", timeout)
	case err = <-done:
		if err != nil {
			// Check if it's an ExitError to get the exit code
			if exitErr, ok := err.(*ssh.ExitError); ok {
				return exitErr.ExitStatus(), string(stdoutBuf), string(stderrBuf), nil
			}
			return 1, string(stdoutBuf), string(stderrBuf), fmt.Errorf("command execution failed: %w", err)
		}
		return 0, string(stdoutBuf), string(stderrBuf), nil
	}
}

// getSSHAuthMethods retrieves SSH authentication methods from secret or default locations
func (c *GuestCommandController) getSSHAuthMethods(
	vmi *v1.VirtualMachineInstance,
	vsockConfigSpec *guestcommandv1alpha1.VSOCKConfigSpec,
	logger *log.FilteredLogger,
) ([]ssh.AuthMethod, error) {
	var authMethods []ssh.AuthMethod

	// If SSHKeySecret is specified, use it
	if vsockConfigSpec != nil && vsockConfigSpec.SSHKeySecret != nil {
		secretName := vsockConfigSpec.SSHKeySecret.Name
		secretKey := vsockConfigSpec.SSHKeySecret.Key
		if secretKey == "" {
			secretKey = "ssh-privatekey"
		}

		secret, err := c.clientset.CoreV1().Secrets(vmi.Namespace).Get(
			context.Background(),
			secretName,
			metav1.GetOptions{},
		)
		if err != nil {
			return nil, fmt.Errorf("failed to get SSH key secret %s: %w", secretName, err)
		}

		keyData, ok := secret.Data[secretKey]
		if !ok {
			return nil, fmt.Errorf("key %s not found in secret %s", secretKey, secretName)
		}

		signer, err := ssh.ParsePrivateKey(keyData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SSH private key from secret: %w", err)
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
		logger.Infof("Using SSH key from secret %s", secretName)
		return authMethods, nil
	}

	// Try default SSH key locations
	home, err := os.UserHomeDir()
	if err != nil {
		home = "/root" // fallback for containerized environments
	}

	keyPaths := []string{
		filepath.Join(home, ".ssh", "id_ed25519"),
		filepath.Join(home, ".ssh", "id_rsa"),
		"/root/.ssh/id_ed25519",
		"/root/.ssh/id_rsa",
	}

	for _, keyPath := range keyPaths {
		if keyData, err := os.ReadFile(keyPath); err == nil {
			signer, err := ssh.ParsePrivateKey(keyData)
			if err == nil {
				authMethods = append(authMethods, ssh.PublicKeys(signer))
				logger.Infof("Using SSH key from %s", keyPath)
				return authMethods, nil
			}
		}
	}

	return nil, fmt.Errorf("no SSH key found. Please specify SSHKeySecret or place a key in ~/.ssh/id_ed25519 or ~/.ssh/id_rsa")
}

// stdoutCapture captures stdout data
type stdoutCapture struct {
	data *[]byte
}

func (c *stdoutCapture) Write(p []byte) (n int, err error) {
	*c.data = append(*c.data, p...)
	return len(p), nil
}

// stderrCapture captures stderr data
type stderrCapture struct {
	data *[]byte
}

func (c *stderrCapture) Write(p []byte) (n int, err error) {
	*c.data = append(*c.data, p...)
	return len(p), nil
}

// escapeShellArg escapes shell arguments for safe execution
func escapeShellArg(arg string) string {
	// Simple escaping: wrap in single quotes and escape single quotes
	escaped := "'"
	for _, r := range arg {
		if r == '\'' {
			escaped += "'\\''"
		} else {
			escaped += string(r)
		}
	}
	escaped += "'"
	return escaped
}

func (c *GuestCommandController) updateCommandStatus(
	command *guestcommandv1alpha1.VirtualMachineGuestCommand,
	phase guestcommandv1alpha1.GuestCommandPhase,
	exitCode int32,
	stdout, stderr, message, reason string,
) (*guestcommandv1alpha1.VirtualMachineGuestCommand, error) {
	commandCopy := command.DeepCopy()
	commandCopy.Status.Phase = phase
	commandCopy.Status.ExitCode = exitCode
	commandCopy.Status.Stdout = stdout
	commandCopy.Status.Stderr = stderr
	commandCopy.Status.Message = message
	commandCopy.Status.Reason = reason
	commandCopy.Status.ObservedGeneration = command.Generation
	now := metav1.Now()
	commandCopy.Status.LastExecutionTime = &now

	// Update condition
	condition := guestcommandv1alpha1.VirtualMachineGuestCommandCondition{
		Type:               guestcommandv1alpha1.GuestCommandConditionExecuted,
		Status:             k8sv1.ConditionTrue,
		LastProbeTime:      now,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}

	// Find and update or append condition
	found := false
	for i, cond := range commandCopy.Status.Conditions {
		if cond.Type == condition.Type {
			commandCopy.Status.Conditions[i] = condition
			found = true
			break
		}
	}
	if !found {
		commandCopy.Status.Conditions = append(commandCopy.Status.Conditions, condition)
	}

	updated, err := c.clientset.VirtualMachineGuestCommand(command.Namespace).UpdateStatus(context.Background(), commandCopy, metav1.UpdateOptions{})
	return updated, err
}
