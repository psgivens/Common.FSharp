module Common.FSharp.Actors.Infrastructure

open Akka.Actor
open Akka.FSharp
open Common.FSharp.Envelopes

type ActorIO<'a> = { Tell:Envelope<'a> -> unit; Actor:IActorRef; Events:IActorRef; Errors:IActorRef }
