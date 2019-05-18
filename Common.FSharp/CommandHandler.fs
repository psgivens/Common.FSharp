module Common.FSharp.CommandHandlers


type CommandHandlerFunction<'state> = ('state -> Async<'state>)

type CommandHandlerBuilder<'event, 'state> (apply:'state -> 'event -> 'state) =
    member this.Bind ((result:Async<'event>), (rest:unit -> CommandHandlerFunction<'state>)) state =
        async {
            let! event = result  
            let state' = apply state event              
            return! rest () state'
        }
    member this.Return (result:Async<Option<'event>>) state = 
        async { 
            let! event = result
            return 
                match event with
                | None -> state
                | Some evt -> apply state evt
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
    

