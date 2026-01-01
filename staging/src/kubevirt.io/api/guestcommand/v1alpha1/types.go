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
	k8sv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VirtualMachineGuestCommand defines a command to be executed in a VM guest via SSH over VSOCK
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=vmgc;vmguestcommand,scope=Namespaced
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="VMI",type="string",JSONPath=".spec.vmiName",description="Target VirtualMachineInstance"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Execution phase"
// +kubebuilder:printcolumn:name="ExitCode",type="integer",JSONPath=".status.exitCode",description="Command exit code"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type VirtualMachineGuestCommand struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of the guest command
	Spec VirtualMachineGuestCommandSpec `json:"spec"`

	// Status defines the observed state of the guest command
	// +optional
	Status VirtualMachineGuestCommandStatus `json:"status,omitempty"`
}

// VirtualMachineGuestCommandSpec describes the configuration for a guest command execution
type VirtualMachineGuestCommandSpec struct {
	// VMIName specifies the target VirtualMachineInstance name
	// +kubebuilder:validation:Required
	VMIName string `json:"vmiName"`

	// Command is the command to execute. The first element is the command path,
	// and the remaining elements are arguments.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	Command []string `json:"command"`

	// Timeout specifies the timeout in seconds for command execution.
	// Defaults to 30 seconds if not specified.
	// +optional
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=300
	Timeout int32 `json:"timeout,omitempty"`

	// RunPolicy specifies when the command should be executed
	// +optional
	// +kubebuilder:default=Once
	RunPolicy GuestCommandRunPolicy `json:"runPolicy,omitempty"`

	// Transport specifies the transport configuration for communicating with the guest.
	// References a VSOCKConfig resource by name.
	// +optional
	Transport *GuestTransport `json:"transport,omitempty"`
}

// GuestCommandRunPolicy defines when the command should run
// +kubebuilder:validation:Enum=Once;OnChange
type GuestCommandRunPolicy string

const (
	// GuestCommandRunOnce executes the command once
	GuestCommandRunOnce GuestCommandRunPolicy = "Once"
	// GuestCommandRunOnChange executes the command when the spec changes
	GuestCommandRunOnChange GuestCommandRunPolicy = "OnChange"
)

// GuestTransport specifies the transport method for communicating with the guest VM.
// Currently only VSOCK is supported.
type GuestTransport struct {
	// VSOCK specifies a reference to a VSOCKConfig resource for SSH over VSOCK transport.
	// +optional
	VSOCK *VSOCKReference `json:"vsock,omitempty"`
}

// VSOCKReference references a VSOCKConfig resource by name in the same namespace.
type VSOCKReference struct {
	// Name is the name of the VSOCKConfig resource in the same namespace.
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// VirtualMachineGuestCommandStatus represents the status of a guest command execution
type VirtualMachineGuestCommandStatus struct {
	// Phase represents the current phase of command execution
	// +optional
	Phase GuestCommandPhase `json:"phase,omitempty"`

	// ExitCode is the exit code returned by the command
	// +optional
	ExitCode int32 `json:"exitCode,omitempty"`

	// Stdout contains the standard output from the command
	// +optional
	Stdout string `json:"stdout,omitempty"`

	// Stderr contains the standard error output from the command
	// +optional
	Stderr string `json:"stderr,omitempty"`

	// Message provides additional information about the execution
	// +optional
	Message string `json:"message,omitempty"`

	// Reason provides a brief CamelCase reason for the phase
	// +optional
	Reason string `json:"reason,omitempty"`

	// LastExecutionTime is the timestamp of the last execution attempt
	// +optional
	LastExecutionTime *metav1.Time `json:"lastExecutionTime,omitempty"`

	// ObservedGeneration reflects the generation of the most recently observed spec
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the command's state
	// +optional
	// +listType=atomic
	Conditions []VirtualMachineGuestCommandCondition `json:"conditions,omitempty" optional:"true"`
}

// VirtualMachineGuestCommandCondition represents a condition of the guest command
type VirtualMachineGuestCommandCondition struct {
	Type               VirtualMachineGuestCommandConditionType `json:"type"`
	Status             k8sv1.ConditionStatus                   `json:"status"`
	LastProbeTime      metav1.Time                             `json:"lastProbeTime,omitempty"`
	LastTransitionTime metav1.Time                             `json:"lastTransitionTime,omitempty"`
	Reason             string                                  `json:"reason,omitempty"`
	Message            string                                  `json:"message,omitempty"`
}

// VirtualMachineGuestCommandConditionType represents the type of condition
type VirtualMachineGuestCommandConditionType string

// GuestCommandPhase represents the phase of guest command execution
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed
type GuestCommandPhase string

const (
	// GuestCommandPending means the command is waiting to be executed
	GuestCommandPending GuestCommandPhase = "Pending"
	// GuestCommandRunning means the command is currently executing
	GuestCommandRunning GuestCommandPhase = "Running"
	// GuestCommandSucceeded means the command completed successfully (exit code 0)
	GuestCommandSucceeded GuestCommandPhase = "Succeeded"
	// GuestCommandFailed means the command failed (non-zero exit code or error)
	GuestCommandFailed GuestCommandPhase = "Failed"
)

// Condition types for VirtualMachineGuestCommand
const (
	// GuestCommandConditionReady indicates the command is ready to execute
	GuestCommandConditionReady VirtualMachineGuestCommandConditionType = "Ready"
	// GuestCommandConditionExecuted indicates the command has been executed
	GuestCommandConditionExecuted VirtualMachineGuestCommandConditionType = "Executed"
)

// VirtualMachineGuestCommandList contains a list of VirtualMachineGuestCommand
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type VirtualMachineGuestCommandList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VirtualMachineGuestCommand `json:"items"`
}

// VSOCKConfig defines VSOCK connection configuration for guest commands
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=vsockcfg,scope=Namespaced
type VSOCKConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the VSOCK configuration
	Spec VSOCKConfigSpec `json:"spec"`
}

// VSOCKConfigSpec defines the configuration for VSOCK/SSH connections
type VSOCKConfigSpec struct {
	// Port specifies the VSOCK port to connect to in the guest.
	// Defaults to 22 (standard SSH port) if not specified.
	// +optional
	// +kubebuilder:default=22
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port uint32 `json:"port,omitempty"`

	// User specifies the SSH user to authenticate as.
	// Defaults to "root" if not specified.
	// +optional
	// +kubebuilder:default="root"
	User string `json:"user,omitempty"`

	// SSHKeySecret is a reference to a secret containing the SSH private key.
	// The SecretKeySelector's Key field specifies which key in the secret contains the private key
	// (commonly "ssh-privatekey").
	// If not specified, the controller will attempt to use default SSH keys from the node.
	// +optional
	SSHKeySecret *k8sv1.SecretKeySelector `json:"sshKeySecret,omitempty"`

	// UseTLS specifies whether to use TLS for the VSOCK connection.
	// Defaults to false if not specified.
	// +optional
	// +kubebuilder:default=false
	UseTLS bool `json:"useTLS,omitempty"`
}

// VSOCKConfigList contains a list of VSOCKConfig
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type VSOCKConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VSOCKConfig `json:"items"`
}
