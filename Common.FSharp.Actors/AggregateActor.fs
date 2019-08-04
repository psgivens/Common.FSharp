﻿[<RequireQualifiedAccess>]
module Common.FSharp.Actors.AggregateActor

open Akka.Actor
open Akka.FSharp

open Common.FSharp.CommandHandlers
open Common.FSharp.Envelopes

type Finished = Finished of TransId

// let private raiseVersionedEvent (self:IActorRef) cmdenv (version:Version) event =
//     let newVersion = incrementVersion version
//     // publish new event
//     let envelope = 
//         reuseEnvelope
//             cmdenv.StreamId
//             newVersion
//             (fun x -> event)
//             cmdenv
//     envelope |> self.Tell        
//     newVersion    



let create<'TState, 'TCommand, 'TEvent> 
    (   eventSubject:IActorRef,
        invalidMessageSubject:IActorRef,
        store:IEventStore<'TEvent>, 
        buildState:'TState option -> 'TEvent list -> 'TState option,
        handle:CommandHandlers<'TEvent, Version> -> 'TState option -> Envelope<'TCommand> -> CommandHandlerFunction<Version>,
        persist:UserId -> StreamId -> 'TState option -> unit) 
    (mailbox:Actor<obj>) =

    let child streamId (mailbox':Actor<obj>) =
        let handleCommand (state, version) cmdenv = 
            async {
                // TODO: Document what is happening here. 

                let raiseVersionedEvent (version:Version) event =
                    let evtenv = Envelope.reuseAndVersionEnvelope cmdenv version event
                    evtenv |> mailbox'.Self.Tell
                    evtenv.Version

                //  raiseVersionedEvent mailbox'.Self cmdenv
                let commandHandlers = CommandHandlers raiseVersionedEvent

                do! cmdenv
                    |> handle commandHandlers state
                    |> Handler.Run version
                    |> Async.Ignore

                // let! events = cmdenv
                //     |> handle commandHandlers state
                //     |> Handler.Run version
                // events |> Seq.iter (mailbox'.Self.Tell)

                // 'stop' case in receiveEvents
                mailbox'.Self <! cmdenv.TransactionId

            } |> Async.Start

        let getState streamId =
            let getVersion e = e.Version 

            let events = 
                store.GetEvents streamId 
                // Crudely remove concurrency errors
                // TODO: Devise error correction mechanism
                |> List.distinctBy getVersion

            let version = 
                if events |> List.isEmpty then Version.box 0s
                else events |> List.last |> getVersion

            // Build current state
            let state = buildState None (events |> List.map unpack)
            state, version

        let rec receiveCommand stateAndVersion = actor {
            let! msg = mailbox'.Receive ()
            return! 
                match msg with 
                | :? Envelope<'TCommand> as cmdenv -> 
                    cmdenv |> handleCommand stateAndVersion
                    [] |> recieveEvents cmdenv.TransactionId stateAndVersion 

                | _ -> stateAndVersion |> receiveCommand
            }
        and recieveEvents transId stateAndVersion events = actor {
            let! msg = mailbox'.Receive ()
            return! 
                match msg with 
                | :? Envelope<'TCommand> as cmdenv -> 
                    mailbox'.Stash ()
                    events |> recieveEvents transId stateAndVersion 

                | :? Envelope<'TEvent> as evtenv ->   
                    if evtenv.TransactionId = transId
                    then evtenv::events |> recieveEvents transId stateAndVersion
                    else failwith 
                        <| sprintf "Envelope expected transaction id %A, but was supplied %A" transId evtenv.TransactionId

                | stop when transId.Equals stop -> 
                    let state, _ = stateAndVersion                    
                    let head = events |> List.head
                    let events' = events |> List.rev
                    let state' = 
                        events'
                        |> List.map (fun env -> env.Item)
                        |> buildState state 
                    
                    events' |> List.iter store.AppendEvent

                    // Query side persistence, record the state.
                    persist head.UserId head.StreamId state'
                
                    events' |> List.iter eventSubject.Tell

                    // Persist and notify events.
                    // Update state.
                    (state', head.Version) |> receiveCommand

                | _ -> events |> recieveEvents transId stateAndVersion
            }
            
        getState streamId |> receiveCommand
    
    let rec loop children = actor {
        let! msg = mailbox.Receive ()

        let getChild streamId =
            match children |> Map.tryFind streamId with
            | Some actor' -> (children,actor')
            | None ->   
                let actorName = mailbox.Self.Path.Parent.Name + "_" + (StreamId.unbox streamId).ToString ()
                let actor' = spawn mailbox actorName <| child streamId                                        
                // TODO: Create timer for expiring cache
                children |> Map.add streamId actor', actor'

        let forward env =
            let children', child = getChild env.StreamId
            child.Forward msg
            children'

        return! 
            match msg with 
            | :? Envelope<'TCommand> as env -> forward env
            | _ -> children
            |> loop
    }
    loop Map.empty<StreamId, IActorRef>



