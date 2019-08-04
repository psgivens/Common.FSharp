module Common.FSharp.CommandHandlers


type CommandHandlerFunction<'state> = ('state -> Async<'state>)

// 'commandHandlerState could be anything with this implementation. In the case of aggregates, it is 
// a Version, and apply increments that version. 
type CommandHandlerBuilder<'event, 'commandHandlerState> (applyDelta:'commandHandlerState -> 'event -> 'commandHandlerState) =
    member this.Bind ((result:Async<'event>), (rest:unit -> CommandHandlerFunction<'commandHandlerState>)) state =
        async {
            let! event = result  
            let state' = applyDelta state event              
            return! rest () state'
        }
    member this.Return (result:Async<Option<'event>>) state = 
        async { 
            let! event = result
            return 
                match event with
                | None -> state
                | Some evt -> applyDelta state evt
        }

[<RequireQualifiedAccess>]
module Handler =
    let Raise event = async { return Some(event) }
    let Empty () = async { return None }
    let Run initialState handler = handler initialState

type CommandHandlers<'event,'state> (raiseVersionedEvent:'state -> 'event -> 'state) =
    member this.block = CommandHandlerBuilder raiseVersionedEvent
    member this.event event = this.block { return event |> Handler.Raise }
    member this.empty state = async { return state }
    

