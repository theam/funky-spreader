namespace Producer

open Kafunk

module SpreaderProducer =
    let create =
        let connection = Kafka.connHost "localhost"

        let producerConfig =
            ProducerConfig.create (
                topic = "spreader",
                partition = Partitioner.roundRobin)

        Producer.createAsync connection producerConfig
        |> Async.RunSynchronously


    let produce producer msg =
        // let kafkaMessage = ProducerMessage.ofString (msg)
        // Producer.produce producer kafkaMessage |> ignore
        printfn "===============> Producing diff: %s" msg

