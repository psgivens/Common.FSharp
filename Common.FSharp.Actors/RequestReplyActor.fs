[<RequireQualifiedAccess>]
module Common.FSharp.Actors.RequestReplyActor

open Akka.Actor
open Akka.FSharp

open Common.FSharp.Envelopes
open Common.FSharp.Actors
open Common.FSharp.Actors.Infrastructure

let spawnRequestReplyConditionalActor<'TCommand,'TEvent> inFilter outFilter sys name (actors:ActorIO<'TCommand>) =
    let actor = spawn sys (name + "_requestreply") <| fun (mailbox:Actor<obj>) ->
        let rec loop senders = actor {
            let! msg = mailbox.Receive ()
            match msg with
            | :? Envelope<'TCommand> as cmdenv ->        
                if inFilter cmdenv then                          
                    cmdenv |> actors.Tell
                    return! loop (senders |> Map.add cmdenv.StreamId (mailbox.Sender ()))                
            | :? Envelope<'TEvent> as evtenv ->
                if outFilter evtenv then 
                    match senders |> Map.tryFind evtenv.StreamId with
                    | Some(sender) -> 
                        sender <! evtenv
                        return! loop (senders |> Map.remove evtenv.StreamId)
                    | None -> ()
            | :? string as value ->
                match value with 
                | "Unsubscribe" -> 
                    mailbox.Self |> SubjectActor.unsubscribeFrom actors.Events
                    mailbox.Self |> mailbox.Context.Stop 
                | _ -> ()
            | _ -> ()
            return! loop senders
        }
        loop Map.empty<StreamId, IActorRef> 
    actor |> SubjectActor.subscribeTo actors.Events
    actor

let spawnRequestReplyActor<'TCommand,'TEvent> sys name (actors:ActorIO<'TCommand>) =
    spawnRequestReplyConditionalActor<'TCommand,'TEvent> 
        (fun x -> true)
        (fun x -> true)
        sys name (actors:ActorIO<'TCommand>) 

