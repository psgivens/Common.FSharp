﻿[<RequireQualifiedAccess>]
module Common.FSharp.Actors.SubjectActor

open Akka.Actor
open Akka.FSharp

type private SubjectAction =
    | Subscribe of IActorRef
    | Unsubscribe of IActorRef

let spawn system name =     
    spawn system name <| fun (mailbox:Actor<obj>) -> 
        let rec loop subscribers = actor {
            let! message = mailbox.Receive ()
            match message with
            | :? SubjectAction as cmd -> 
                match cmd with
                | Subscribe actor -> 
                    return! loop 
                        <|  match subscribers 
                                |> List.tryFind (fun (a:IActorRef) -> 
                                    actor.Path = a.Path) 
                                with
                            | None -> actor::subscribers
                            | Some(_) -> subscribers
                | Unsubscribe actor -> 
                    return! loop 
                        (subscribers 
                         |> List.filter (fun item -> 
                            item <> actor))        
            | _ -> 
                // publish to all subscribers
                let tellSubscriber (actor:IActorRef) = actor.Tell message
                subscribers |> List.iter tellSubscriber
                return! loop subscribers
        }        
        loop []
    

let subscribeTo (events:IActorRef)  =
    Subscribe >> events.Tell

let unsubscribeFrom (events:IActorRef)  =
    Unsubscribe >> events.Tell
