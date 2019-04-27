module Common.FSharp.CommandHandlers

type CommandHandlerFunction<'state> = ('state -> Async<'state>)

type CommandHandlerBuilder<'event, 'state> (raise:'state -> 'event -> 'state) =
    member this.Bind ((result:Async<'event>), (rest:unit -> CommandHandlerFunction<'state>)) =
        fun state -> 
            async {
                let! event = result                
                return! (rest ()) (raise state event)
            }
    member this.Return (result:Async<'event>) = 
        fun state -> 
            async { 
                let! event = result
                return raise state event
            }

[<RequireQualifiedAccess>]
module Handler =
    let Raise event = async { return event }
    let Run initialState handler = handler initialState

type CommandHandlers<'event,'state> (raiseVersionedEvent:'state -> 'event -> 'state) =
    member this.block = CommandHandlerBuilder raiseVersionedEvent
    member this.event event = this.block { return event |> Handler.Raise }
    

