module Common.FSharp.EventSourceGherkin

open Common.FSharp.Envelopes

type Preconditions<'TEvent,'TState> = 
    | State of 'TState option
    | Events of 'TEvent list 

type SystemUnderTest<'TCommand,'TEvent> =
    | Events of 'TEvent list
    // | Command of 'TCommand

type TestResults<'TEvent,'TState> = 
    | OK of 'TEvent * 'TState option
    | Error of System.Exception

type PostConditions<'TEvent,'TState> = {
    Events: 'TEvent list option
    State: 'TState option option
    Error: System.Exception option }

let expectState state =
    {
        Events = None
        State = state |> Some
        Error = None
    }

let expectEvents events=
    {
        Events = events
        State = None
        Error = None
    }

let expectError error =
    {
        Events = None
        State = None
        Error = error |> Some
    }

let doNotPersist<'a> (uid:UserId) (sid:StreamId) (state:'a option) = ()

let runWaitAndIgnore (task:'a when 'a :> System.Threading.Tasks.Task<'b>) = 
    task
    |> Async.AwaitTask
    |> Async.Ignore
    |> Async.RunSynchronously


type TestFailure (error) = 
    inherit System.Exception (error)
   
type TestConditions<'TCommand,'TEvent,'TState when 'TEvent : equality and 'TState : equality>  
        (buildInitialState, 
         buildState:('TState option -> 'TEvent list -> 'TState option)
        ) = 

    let testFailure (name:string, expected:'a, actual:'a) = 
        // TODO: Create better error message
        sprintf "%s\n\texpected: %A\n\tactual: %A" name expected actual
        |> TestFailure 
        |> raise  
    
    let testException name (expected:System.Exception option) (actual:System.Exception) = 
        match expected with
        | Some(value) -> 
            if actual.GetType() <> value.GetType()  && actual.Message <> value.Message then 
                testFailure(name, value, actual) 
        | None -> ()
    
    let test name (expected:'a option) (actual:'a) = 
        match expected with
        | Some(value) -> 
            if actual <> value then 
                testFailure(name, value, actual) 
        | None -> ()
    
    member this.Given (preconditions: Preconditions<'TEvent,'TState>) =
        preconditions

    member this.When 
            (sut: SystemUnderTest<'TCommand,'TEvent>) 
            (preconditions:Preconditions<'TEvent,'TState>) = 
        let execute () = 
            let preState = 
                match preconditions with
                | Preconditions.Events(events) -> 
                    buildInitialState events
                | State(state) -> state

            try 
                match sut with
                | Events(events) -> 
                    let state = events |> buildState preState 
                    OK(events,state)
            with
            | ex -> Error(ex)
        
        execute


    member this.Then (expected:PostConditions<'TEvent,'TState>) execute  = 
    
        // validate preconditions
        if (Option.isSome expected.Error
            && (Option.isSome expected.State || Option.isSome expected.Events))
            || (Option.isNone expected.Error && Option.isNone expected.State && Option.isNone expected.Events) then
                failwith "Invalid postconditions"
    
        match execute () with 
        | OK(events,state) ->
            events |> test "events" expected.Events
            state  |> test "state" expected.State 
        | Error(ex) ->
            if Option.isSome expected.State then failwith <| sprintf "Expected a state.\n\terror:%A" ex
            if Option.isSome expected.Events then failwith <| sprintf "Expected events.\n\terror:%A" ex
            ex |> testException "error" expected.Error
    
module TestData =
    
    let nullPostConditions = {
        State = None
        Events = None
        Error = None
    }

type InMemoryEventStore<'a> =
  val mutable events : Map<StreamId, Envelope<'a> list> 
  new () = { events = Map.empty }
  new eventMap = { events = eventMap }
  interface IEventStore<'a> with
    member this.GetEvents (streamId:StreamId) =
      match this.events |> Map.tryFind streamId with
      | Some (events') -> 
        events'
        |> Seq.toList 
        |> List.sortBy(fun x -> x.Version)
      | None -> []
    member this.AppendEvent (envelope:Envelope<'a>) =
      let store = this :> IEventStore<'a>
      let events' = store.GetEvents envelope.StreamId
      this.events <- this.events |> Map.add envelope.StreamId (envelope::events')

module Tests =
  let userId = UserId.create ()
  let envelop<'a> streamId (payload:'a) = 
    envelopWithDefaults 
        userId 
        (TransId.create ()) 
        streamId 
        payload
  let envelopi<'a> streamId (i:int) (payload:'a)=
    envelope 
        userId 
        (TransId.create ()) 
        (System.Guid.NewGuid()) 
        (Version.box (int16 i))
        (System.DateTimeOffset.Now) 
        payload
        streamId
