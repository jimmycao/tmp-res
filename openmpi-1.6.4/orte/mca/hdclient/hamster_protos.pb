
�
hamster_protols.proto"/
ProcessNameProto
jobid (
vpid ("E
HamsterHnpRequestProto
request (
msg_type (2.MsgType"4
NodeResourceProto
	host_name (	
slot (".
AllocateRequestProto
resource_count ("C
AllocateResponseProto*
node_resources (2.NodeResourceProto"f
LaunchContextProto
envars (	
args (	
	host_name (	
name (2.ProcessNameProto"E
LaunchResultProto
name (2.ProcessNameProto
success ("B
LaunchRequestProto,
launch_contexts (2.LaunchContextProto":
LaunchResponseProto#
results (2.LaunchResultProto"
HeartbeatRequestProto"l
ProcessStatusProto
name (2.ProcessNameProto!
state (2.ProcessStateProto

exit_value ("J
HeartbeatResponseProto0
completed_processes (2.ProcessStatusProto"
RegisterRequestProto"
RegisterResponseProto":
FinishRequestProto
succeed (
diagnostics (	"
FinishResponseProto*Y
MsgType
UNKNOWN 
ALLOCATE

LAUNCH
REGISTER

FINISH
	HEARTBEAT*/
ProcessStateProto
RUNNING
	COMPLETEDB0
com.pivotal.hamster.protoBHamsterProtos��