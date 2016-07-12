namespace codecraft.platform.amqp

open System
open System.Text
open System.Threading
open codecraft.codegen
open RabbitMQ.Client
open RabbitMQ.Client.Events
open codecraft.platform
open Newtonsoft.Json
open System.Collections.Generic

module AmqpCloud =
  let enc = Encoding.UTF8

  type RespLetter = {
    caseType: string
    statePayload: byte array
  }

  type State =
    | BackoffState of BackoffState
    | FailedState of FailedState
    | SuccessState of SuccessState
  and BackoffState = {
    millis: int
  }
  and FailedState = {
    message: string
  }
  and SuccessState = {
    payload: byte array
  }

  let deserializeLetter (bytes: byte array) =
    try
      let data = enc.GetString bytes
      printfn "letter = %s" data

      let letter = JsonConvert.DeserializeObject<RespLetter>(data)
      let payloadData = enc.GetString letter.statePayload
      printfn "Payload data = %s" payloadData

      match letter.caseType with
      | "backoff" -> JsonConvert.DeserializeObject<BackoffState>(payloadData) |> BackoffState
      | "failed" -> JsonConvert.DeserializeObject<FailedState>(payloadData) |> FailedState
      | "success" -> JsonConvert.DeserializeObject<SuccessState>(payloadData) |> SuccessState
      |> Choice1Of2
    with e -> Choice2Of2 e

  let serializeLetter letter =
    let caseType, data =
      match letter with
      | BackoffState state -> "success", JsonConvert.SerializeObject(state)
      | FailedState state -> "failed", JsonConvert.SerializeObject(state)
      | SuccessState state -> "success", JsonConvert.SerializeObject(state)
    let letter = {
      caseType = caseType
      statePayload = data |> enc.GetBytes
    }
    JsonConvert.SerializeObject(letter) |> enc.GetBytes

  type private Task (id, info) =
    let mut = new AutoResetEvent(false)
    let res = "Not defined" |> Exception |> Choice2Of2 |> ref
    let task =
      async {
        mut.WaitOne() |> ignore
        return !(res)
      } |> Async.StartAsTask

    member this.Resolve (value:obj) =
      res := value |> Choice1Of2
      mut.Set() |> ignore

    member this.Id = id
    member this.Info = info

    member this.Await () = async {
      let! res = task |> Async.AwaitTask
      return res
    }

  type private AmqpCloud(routingInfo: RoutingInfo, uri: Uri, username: string, password: string, vhost: string) =
    let connections =
      let connections = ConnectionFactory()
      connections.Endpoint <- uri |> AmqpTcpEndpoint
      connections.UserName <- username
      connections.Password <- password
      connections.VirtualHost <- vhost
      connections

    do printfn "Connecting to %A" uri
    let conn = connections.CreateConnection()
    do printfn "Connected"
    let chan = conn.CreateModel()

    let groupChansLock = new Object()
    let groupChans = Map.empty<string, IModel> |> ref

    // Setup private consumer.
    let privateQueue = chan.QueueDeclare()
    let privateConsumer = EventingBasicConsumer()
    let taskLock = new Object()
    let tasks = Map.empty<string, Task> |> ref

    let cmdSubscriptionLock = new Object()
    let cmdSubscriptions = Map.empty<string, CmdGroupConsumer> |> ref

    do
      privateConsumer.add_Received(fun model args ->
        let bytes = args.Body
        let id = args.BasicProperties.CorrelationId
        let task = lock taskLock (fun () ->
          let _tasks = !tasks
          let task = Map.tryFind id _tasks
          tasks := Map.remove id _tasks
          task
        )

        match task with
        | None ->
          let data = enc.GetString bytes
          printfn "Received invalid response data %s" data
        | Some task ->
          match deserializeLetter bytes with
          | Choice2Of2 e -> printfn "Response %s failed with: %A" id e
          | Choice1Of2 letter ->
            printfn "Letter = %A" letter
            match letter with
            | FailedState res ->
              printfn "Response failed with %A" res
            | BackoffState res ->
              printfn "Response failed with %A" res
            | SuccessState res ->
              printfn "Success res = %s" (enc.GetString res.payload)
              let info = task.Info
              match info.deserializeReply res.payload with
              | Choice2Of2 e -> printfn "Response %s failed with: %A" id e
              | Choice1Of2 msg ->
                printfn "Data response %A" msg
                task.Resolve msg
      )
      chan.BasicConsume(privateQueue, true, "", false, true, null, privateConsumer) |> ignore

    let fromNull a =
      match a with
      | null -> None
      | _ -> Some a

    let deliveryFailure (deliveryTag, correlationId, replyTo, message:string) =
      let data = message |> enc.GetBytes
      let props = chan.CreateBasicProperties()
      props.CorrelationId <- correlationId

      chan.BasicPublish(
        "",
        replyTo,
        props,
        data
      )
      chan.BasicAck(
        deliveryTag,
        false
      )

    let deliverySuccess (deliveryTag, correlationId, replyTo, data:byte array) =
      let props = chan.CreateBasicProperties()
      props.CorrelationId <- correlationId

      chan.BasicPublish(
        "",
        replyTo,
        props,
        data
      )
      chan.BasicAck(
        deliveryTag,
        false
      )
      printfn "Published deliverySuccess for %s" correlationId

    let startGroupConsumer (chan: IModel) (info: GroupRouting) =
      let args: IDictionary<string, obj> = null
      chan.QueueDeclare(
        info.queueName,
        false,
        true,
        false,
        false,
        true,
        null
      ) |> ignore
      chan.BasicQos(uint32 0, uint16 1, false)
      chan.QueueBind(
        info.queueName,
        info.exchange,
        info.routingKey,
        false,
        null
      )

      let consumer = new QueueingBasicConsumer(chan)
      chan.BasicConsume(
        info.queueName,
        false,
        null,
        consumer
      ) |> ignore

      let rec loop () = async {
        printfn "Pulling message from queue %s" info.queueName
        let args = consumer.Queue.Dequeue() :?> BasicDeliverEventArgs
        printfn "Received cmd from %s" info.queueName
        let props = args.BasicProperties
        let cmdKey = args.RoutingKey

        printfn "Handling command %s on %s" cmdKey info.queueName

        async {
          let info = routingInfo.cmd |> Map.tryFind cmdKey
          printfn "Got routing info for %s" cmdKey

          if info |> Option.isNone then
            sprintf "Routing key %s has undefined info" cmdKey
            |> Exception
            |> Choice2Of2
          else

          let info = info |> Option.get
          let group = info.group
          let replyTo = props.ReplyTo |> fromNull
          let correlationId = props.CorrelationId |> fromNull
          let deliveryTag = args.DeliveryTag

          if replyTo |> Option.isNone then
            sprintf "ReplyTo was undefined in delivery"
            |> Exception
            |> Choice2Of2
          else

          if correlationId |> Option.isNone then
            sprintf "CorrelationId was undefined in delivery"
            |> Exception
            |> Choice2Of2
          else

          let replyTo = replyTo |> Option.get
          let correlationId = correlationId |> Option.get

          match !cmdSubscriptions |> Map.tryFind group.queueName with
          | None ->
            printfn "Not subsciption found for %s under %s" cmdKey group.queueName
            deliveryFailure (deliveryTag, correlationId, replyTo, "No subscribers found -- Nack not implemented")
          | Some sub ->
            printfn "Pushing command %s to subscriber" cmdKey

            match info.deserializeCmd args.Body with
            | Choice2Of2 e ->
              printfn "Unable to deserialize cmd %s" cmdKey
              deliveryFailure (deliveryTag, correlationId, replyTo, sprintf "DeserializeCmd error: %A" e)
            | Choice1Of2 cmd ->
              printfn "Received command %A" cmd
              // Now, pipe the command to the subscriber.
              let subMethod = sub.methodRegistry |> Map.tryFind cmdKey
              if subMethod |> Option.isNone then
                deliveryFailure (deliveryTag, correlationId, replyTo, sprintf "Method not found: %s" info.key)
              else

              try
                let subMethod = subMethod |> Option.get
                let reply = subMethod cmd
                printfn "Got reply %A from subscriber for cmd %s" reply cmdKey

                match info.serializeReply reply with
                | Choice2Of2 e ->
                  printfn "DeliveryFailure: %A" e
                  deliveryFailure (deliveryTag, correlationId, replyTo, sprintf "SerializeReply error: %A" e)
                | Choice1Of2 data ->
                  let letter = SuccessState { payload = data }
                  let letterData = serializeLetter letter
                  printfn "Delivery success for cmd %A with %A" cmd reply
                  deliverySuccess (deliveryTag, correlationId, replyTo, letterData)
              with e ->
                printfn "DeliveryFailure: %A" e
                deliveryFailure (deliveryTag, correlationId, replyTo, sprintf "HandlerError: %A" e)

        } |> Async.Start
        return! loop ()
      }
      loop () |> Async.Start

    interface ICloud with
      member this.requestCmd cmdKey cmd timeout = async {
        let info = routingInfo.cmd |> Map.tryFind cmdKey
        if Option.isNone info then
          let res: Choice<obj, exn> = sprintf "InvalidArg: cmdKey %s has no routing info" cmdKey |> exn |> Choice2Of2
          return res
        else

        let info = Option.get info
        let data = info.serializeCmd cmd
        match data with
        | Choice2Of2 e ->
          return Choice2Of2 e

        | Choice1Of2 data ->
          let id = Guid.NewGuid().ToString()

          let task = Task(id, info)
          lock taskLock (fun () ->
            tasks := Map.add id task (!tasks)
          )
          // Route the message.
          let props = chan.CreateBasicProperties()
          props.ReplyTo <- privateQueue
          props.CorrelationId <- id

          chan.BasicPublish(info.group.exchange, info.key, props, data)

          let! res = task.Await()

          return res
      }

      member this.subscribeCmd groupKey service timeout: Choice<unit, exn> Async = async {
        let groupInfo = routingInfo.group |> Map.tryFind groupKey
        if groupInfo |> Option.isNone then
          return sprintf "GroupKey %s has no routing info" groupKey |> Exception |> Choice2Of2
        else
        let groupInfo = groupInfo |> Option.get
        let chan = lock groupChansLock (fun () ->
          match !groupChans |> Map.tryFind groupKey with
          | Some chan -> chan
          | None ->
            let chan = conn.CreateModel()
            groupChans := !groupChans |> Map.add groupKey chan
            chan
        )
        lock cmdSubscriptionLock (fun () ->
          cmdSubscriptions := !cmdSubscriptions |> Map.add groupInfo.queueName service
        )
        return startGroupConsumer chan groupInfo |> Choice1Of2
      }

      member this.disconnect () = ()

  let cloud (routingInfo: RoutingInfo) uri username password vhost =
    AmqpCloud(routingInfo, uri, username, password, vhost) :> ICloud
