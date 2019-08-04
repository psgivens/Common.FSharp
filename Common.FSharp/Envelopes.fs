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

let buildState (evolve:'s option -> 'evt -> 's option) = List.fold evolve 

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
            item =
        streamId 
        |> envelope 
            userId 
            transId 
            (Guid.NewGuid()) 
            (Version.box 0s)
            (DateTimeOffset.Now) 
            item

    let reuseEnvelope<'a,'b> streamId (version:Version) (func:'a->'b) (envelope:Envelope<'a>) ={
        Id = envelope.Id
        UserId = envelope.UserId
        StreamId = streamId
        Version = version
        Created = envelope.Created
        Item = func envelope.Item
        TransactionId = envelope.TransactionId 
        }
        
    let unpack envelope = envelope.Item

    let reuseAndVersionEnvelope cmdenv (version:Version) event =
        reuseEnvelope
            cmdenv.StreamId
            (incrementVersion version)
            (fun x -> event)
            cmdenv

type InvalidCommand (state:obj, command:obj) =
    inherit System.Exception(sprintf "Invalid command.\n\tcommand: %A\n\tstate: %A" command state)
   
type InvalidEvent (state:obj, event: obj) =
    inherit System.Exception(sprintf "Invalid event.\n\event: %A\n\tstate: %A" event state)

type IEventStore<'T> = 
    abstract member GetEvents: StreamId -> Envelope<'T> list
    abstract member AppendEvent: Envelope<'T> -> unit
