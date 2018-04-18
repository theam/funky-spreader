open System
open System.IO
open System.Threading
open System.Diagnostics

open Kafunk
open FSharpx.Choice

open FParsec
open Parser


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
    | MessageProduced


let diffChars (equals : seq<char>, differents : seq<char>) (x : char) (y : char) : (seq<char> * seq<char>) =
    if x = y && y <> ' '
    then ((Seq.singleton x)|> Seq.append equals , differents)
    else (equals, (Seq.singleton y) |> Seq.append differents )


let diffLogs ( oldLog : string list ) ( newLog : string list) : string list =
    newLog
    |> Seq.skip oldLog.Length
    |> Seq.toList


let eventHandler state ev =
    match ev with
    | AppStarted ->
        (state, ReadBinLog state.BinLogFile)

    | BinLogRead(newLog) ->
        let diff = diffLogs state.OldLog newLog
        if diff.Length = 0
        then
            printfn "Diff is empty"
            (state, NoOp)
        else
            let newState = { state with OldLog = newLog }
            (newState, PublishLog(diff))

    | MessageProduced ->
        (state, NoOp)

let readBinLog filepath =
    let proc = new Process()
    let cmd  = "mysqlbinlog"
    proc.StartInfo.UseShellExecute        <- false
    proc.StartInfo.FileName               <- cmd
    proc.StartInfo.Arguments              <- " -t --database=spreader " + filepath
    proc.StartInfo.CreateNoWindow         <- true
    proc.StartInfo.RedirectStandardOutput <- true
    proc.Start() |> ignore
    proc.StandardOutput.ReadToEnd()


let effectHandler state ev =
    match ev with
    | ReadBinLog(filepath) -> choose {
        // let! content = protect File.ReadAllText filepath
        let! content = protect readBinLog filepath
        match BinLog.parse content with
        | Failure(s, err, _) ->
            printfn "Parse error: %s" (err.ToString())
            return (BinLogRead [""])
        | Success(res, _, _) ->
            return (BinLogRead res)
        }

    | PublishLog(msgs) -> choose {
        // let kafkaMessage = ProducerMessage.ofString (msg)
        // Producer.produce state.KafkaProducer kafkaMessage |> ignore
        msgs
        |> Seq.iter (printfn "===============> Producing diff: %s")
        return MessageProduced
        }

    | _ -> Choice2Of2 (Exception("Panic: This shouldn't have happened"))



let rec runApp evHandler effHandler state ev =
    match ev with
    | Choice2Of2 _ ->
        state

    | Choice1Of2 other ->
        let (newState, eff) = evHandler state other
        let next            = effHandler newState eff
        runApp evHandler effHandler newState next


let rec loopApp evHandler effHandler state =
    let newState = runApp evHandler effHandler state ( Choice1Of2 AppStarted )
    "Waiting..............\n" |> Seq.iter (fun c -> printf "%c" c; Thread.Sleep 100)
    loopApp evHandler effHandler newState




[<EntryPoint>]
let main argv =
    let connection = Kafka.connHost "localhost"

    let producerConfig =
        ProducerConfig.create (
            topic = "spreader",
            partition = Partitioner.roundRobin)

    let producer =
        Producer.createAsync connection producerConfig
        |> Async.RunSynchronously

    let initialState =
        { BinLogFile    = "/var/lib/mysql/mysql-bin.000009";
          OldLog        = [""];
          KafkaProducer = producer; }

    loopApp eventHandler effectHandler initialState
    0
