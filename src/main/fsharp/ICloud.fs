namespace codecraft.platform

open codecraft.codegen

type RoutingInfo = {
  cmd: Map<string, CmdRegistry>
  group: Map<string, GroupRouting>
}

type CmdGroupKey =
  | UserStoreCmdGroupKey

[<Interface>]
type ICloud =
  abstract member requestCmd: cmdKey:string -> cmd:obj -> timeout:int -> Choice<obj, exn> Async
  abstract member subscribeCmd: groupKey:string -> service:CmdGroupConsumer -> timeout:int -> Choice<unit, exn> Async
  abstract member disconnect: unit -> unit
