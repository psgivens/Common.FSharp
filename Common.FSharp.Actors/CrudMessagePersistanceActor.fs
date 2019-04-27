[<RequireQualifiedAccess>]
module Common.FSharp.Actors.CrudMessagePersistanceActor

open Akka.Actor
open Akka.FSharp

open Common.FSharp.Envelopes

let private create<'TCommand> 
    (   eventSubject:IActorRef,
        errorSubject:IActorRef,
        persist:UserId -> StreamId -> Envelope<'TCommand> option -> unit) =

    let persistEntity (mailbox:Actor<Envelope<'TCommand>>) (envelope:Envelope<'TCommand>) =
        try
            persist
                envelope.UserId
                envelope.StreamId
                (Some(envelope))
                
            envelope 
            |> Envelope.reuseEnvelope envelope.StreamId ignore
            |> eventSubject.Tell
        with
            | ex -> errorSubject <! ex
                                        
    actorOf2 persistEntity

open Common.FSharp.Actors.Infrastructure
let spawn<'TState> 
   (sys,
    name,
    persist:UserId -> StreamId -> Envelope<'TState> option -> unit) = 
    // Create a subject so that the next step can subscribe. 
   let persistEntitySubject = SubjectActor.spawn sys (name + "_Events")
   let errorSubject = SubjectActor.spawn sys (name + "_Errors")
   let messagePersisting = 
       create<'TState>
           (persistEntitySubject,
            errorSubject,
            persist)
       |> spawn sys (name + "_PersistingActor")

   { Tell=fun (env:Envelope<'TState>) -> env |> messagePersisting.Tell; 
     Actor=messagePersisting;
     Events=persistEntitySubject;
     Errors=errorSubject }

