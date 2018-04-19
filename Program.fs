open System
open System.IO
open System.Threading

open Kafunk
open FSharpx.Choice
open FSharpx.Option
open FParsec

open LogDiff
open BinLog
open Parser
open Producer


type State =
    { BinLogFile    : string;
      OldLog        : string list;
      KafkaProducer : Producer; }


type Effect =
    | ReadBinLog of string
    | PublishLog of string list
    | NoOp


type Event =
    | AppStarted
    | BinLogRead of string list
    | AppErrored of string
    | MessageProduced

type EventHandler  = State -> Event  -> (State * Effect)

type EffectHandler = State -> Effect -> (State * Event) option



let eventHandler ( state : State ) ( ev : Event ) : ( State * Effect ) =
    match ev with
    | AppStarted ->
        (state, ReadBinLog state.BinLogFile)

    | BinLogRead(newLog) ->
        let diff = LogDiff.diff state.OldLog newLog
        if diff.Length = 0
        then
            (state, NoOp)
        else
            ({ state with OldLog = newLog }, PublishLog(diff))

    | AppErrored(errorMessage) ->
        (state, NoOp)

    | MessageProduced ->
        (state, NoOp)


let effectHandler ( state : State ) ( eff : Effect ) : ( State * Event ) option =
    match eff with
    | ReadBinLog(filepath) -> maybe {
        let content = BinLog.read filepath
        match BinLog.parse content with
        | Failure(s, err, _) ->
            return (state, AppErrored (err.ToString()) )
        | Success(res, _, _) ->
            return (state, BinLogRead res )
        }

    | PublishLog(msgs) -> maybe {
        msgs |> Seq.iter (SpreaderProducer.produce state.KafkaProducer)
        return (state, MessageProduced )
        }

    | _ -> None


let rec runApp ( evHandler : EventHandler ) ( effHandler : EffectHandler ) ( state: State ) ( ev : Event ) : State =
    evHandler state ev
    ||> effHandler
    |>  Option.map (fun (s, e) -> runApp evHandler effHandler s e)
    |>  getOrElse state


let rec loopApp evHandler effHandler state =
    runApp evHandler effHandler state AppStarted
    |> loopApp evHandler effHandler


[<EntryPoint>]
let main argv =
    if argv.Length < 1
    then
        printfn "You must provide the binary log file as the first argument"
        1
    else
        let initialState =
            { BinLogFile    = argv.[0];
              OldLog        = [""];
              KafkaProducer = SpreaderProducer.create; }

        loopApp eventHandler effectHandler initialState
        0
