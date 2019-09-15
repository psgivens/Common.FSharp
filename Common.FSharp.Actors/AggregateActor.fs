[<RequireQualifiedAccess>]
module Common.FSharp.Actors.AggregateActor

open Akka.Actor
open Akka.FSharp

open Common.FSharp.CommandHandlers
open Common.FSharp.Envelopes

type Finished = Finished of TransId

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
                // Creates a function for telling this actor that an event has been raised.
                // The event sourcing infrastructure will take care of creating the new 
                // version to pass to this method.
                let raiseVersionedEvent (version:Version) event =
                    let evtenv = Envelope.reuseAndVersionEnvelope cmdenv version event
                    evtenv |> mailbox'.Self.Tell
                    evtenv.Version

                //  raiseVersionedEvent mailbox'.Self cmdenv
                let commandHandlers = CommandHandlers raiseVersionedEvent

                // This kicks off the domain command handler which may produce 
                // several events asynchronously. After `handleCommand` is started
                // the actor moves to a receiving events state. 
                do! cmdenv
                    |> handle commandHandlers state
                    |> Handler.Run version
                    |> Async.Ignore

                // When the domain command handler finishes, we need to trigger a 
                // move back to receiving commands. 
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

                    // Process this command
                    cmdenv |> handleCommand stateAndVersion

                    // Transition this actor to receiving events
                    [] |> recieveEvents cmdenv.TransactionId stateAndVersion 

                // If we are receving commands, we can ignore anything else
                // because we are not generated events.
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

        // For each aggregate, we create a child to handle the command, and alll
        // events coming from that command. If another command comes in while we 
        // are handling the first command, it will be directed to the same child
        // actor and stashed. 
        //
        // **Why not just the ConsistentHashingPool?**
        // An actor in the pool may handle commands for many different aggregates,
        // whereas, our child aggregate handles commands & and events for only 
        // on aggregate. This is necessary because aggregate actor needs to maintain
        // the state of the aggregate as it processes the command and events. 

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



