module Common.FSharp.Envelopes

open System

type FsGuidType = Id of Guid with
    static member create () = Id (Guid.NewGuid ())
    static member unbox (Id(id)) = id
    static member box id = Id(id)
    static member Empty = Id (Guid.Empty)
    static member toString (Id(id)) = id.ToString () 

type FsType<'T> = Val of 'T with
    static member box value = Val(value)
    static member unbox (Val(value)) = value

type StreamId = FsGuidType
type TransId = FsGuidType
type UserId = FsGuidType
type Version = FsType<int16> 

let incrementVersion version = 
    let value = Version.unbox version
    Version.box <| value + 1s

let buildState evolve = List.fold (fun st evt -> Some (evolve st evt))

let defaultDate = "1900/1/1" |> DateTime.Parse
let interpret naMessage (format:string) (date:DateTime) =
    if date = defaultDate then naMessage else date.ToString format

[<AutoOpen>]
module Envelope =

    [<CLIMutable>]
    type Envelope<'T> = {
        Id: Guid
        UserId: UserId
        StreamId: StreamId
        TransactionId: TransId
        Version: Version
        Created: DateTimeOffset
        Item: 'T 
        }

    let envelope 
            userId 
            transId 
            id 
            version 
            created 
            item 
            streamId = {
        Id = id
        UserId = userId
        StreamId = streamId
        Version = version
        Created = created
        Item = item
        TransactionId = transId 
        }
        
    let envelopWithDefaults 
            (userId:UserId) 
            (transId:TransId) 
            (streamId:StreamId) 
            (version:Version) 
            item =
        streamId 
        |> envelope 
            userId 
            transId 
            (Guid.NewGuid()) 
            version 
            (DateTimeOffset.Now) 
            item

    let reuseEnvelope<'a,'b> streamId (func:'a->'b) (envelope:Envelope<'a>) ={
        Id = envelope.Id
        UserId = envelope.UserId
        StreamId = streamId
        Version = envelope.Version
        Created = envelope.Created
        Item = func envelope.Item
        TransactionId = envelope.TransactionId 
        }
        
    let unpack envelope = envelope.Item

type InvalidCommand (state:obj, command:obj) =
    inherit System.Exception(sprintf "Invalid command.\n\tcommand: %A\n\tstate: %A" command state)
   
type InvalidEvent (state:obj, event: obj) =
    inherit System.Exception(sprintf "Invalid event.\n\event: %A\n\tstate: %A" event state)

type IEventStore<'T> = 
    abstract member GetEvents: StreamId -> Envelope<'T> list
    abstract member AppendEvent: Envelope<'T> -> unit
