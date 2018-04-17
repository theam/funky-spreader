open System
open System.IO
open System.Threading

open Kafunk
open FSharpx.Choice


type State =
    { BinLogFile    : string;
      OldLog        : string;
      KafkaProducer : Producer; }


type Effect =
    | ReadBinLog of string
    | PublishLog of string
    | NoOp


type Event =
    | AppStarted
    | BinLogRead of string
    | MessageProduced


let diffChars (equals : seq<char>, differents : seq<char>) (x : char) (y : char) : (seq<char> * seq<char>) =
    if x = y
    then (equals |> Seq.append (Seq.singleton x), differents)
    else (equals, differents |> Seq.append (Seq.singleton y))


let diffLogs oldLog newLog =
    let oL                   = oldLog |> Seq.map char
    let nL                   = newLog |> Seq.map char
    let (equals, differents) = Seq.fold2 diffChars ("" |> Seq.map char, "" |> Seq.map char) oL nL
    differents |> Seq.map string |> String.concat ""


let eventHandler state ev =
    match ev with
    | AppStarted ->
        (state, ReadBinLog state.BinLogFile)

    | BinLogRead(newLog) ->
        let diff = diffLogs state.OldLog newLog
        if diff = ""
        then (state, NoOp)
        else ({ state with OldLog = diff }, PublishLog(diff))

    | MessageProduced ->
        (state, NoOp)


let effectHandler state ev =
    match ev with
    | ReadBinLog(filepath) -> choose {
        let! content = protect File.ReadAllText filepath
        return (BinLogRead content)
        }

    | PublishLog(msg) -> choose {
        let kafkaMessage = ProducerMessage.ofString (msg)
        // Producer.produce state.KafkaProducer kafkaMessage |> ignore
        printfn "%s" msg
        return MessageProduced
        }

    | _ -> Choice2Of2 (new Exception("Panic: This shouldn't have happened"))


let rec runApp evHandler effHandler state ev =
    match ev with
    | None ->
        printfn "Application ended"

    | Some(other) ->
        let (newState, eff) = evHandler state other
        let next            = effHandler newState eff |> toOption
        Thread.Sleep 10
        runApp evHandler effHandler newState next



[<EntryPoint>]
let main argv =
    let connection = Kafka.connHost "localhost"

    let producerConfig =
        ProducerConfig.create (
            topic = "spreader",
            partition = Partitioner.roundRobin,
            requiredAcks = RequiredAcks.Local )

    let producer =
        Producer.createAsync connection producerConfig
        |> Async.RunSynchronously

    let initialState =
        { BinLogFile    = "/home/nick/.mysql-binlogs/binlog";
          OldLog        = "";
          KafkaProducer = producer; }
    runApp eventHandler effectHandler initialState ( Some AppStarted )
    0
