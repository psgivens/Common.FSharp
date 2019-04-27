[<RequireQualifiedAccess>]
module Common.FSharp.Actors.PersistanceActor

open Akka.Actor
open Akka.FSharp

open Common.FSharp.Envelopes

let create<'TState, 'TCommand, 'TEvent> 
    (   eventSubject:IActorRef,
        errorSubject:IActorRef,
        store:IEventStore<'TEvent>, 
        buildState:'TState option -> 'TEvent list -> 'TState option,
        persist:UserId -> StreamId -> 'TState option -> unit) =

    let persistEntity (mailbox:Actor<Envelope<'TEvent>>) (envelope:Envelope<'TEvent>) =
        try
            // Retrieve existing events
            let events = 
                store.GetEvents envelope.StreamId
                // Crudely remove concurrency errors
                |> List.distinctBy (fun e -> e.Version)
                
            // Build current state
            let state = buildState None (events |> List.map unpack)

            persist envelope.UserId envelope.StreamId state
            eventSubject <! envelope  

        with
            | ex -> errorSubject <! ex
                                        
    actorOf2 persistEntity
        
