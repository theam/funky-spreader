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
        let kafkaMessage = ProducerMessage.ofString (msg)
        let res =
            Producer.produce producer kafkaMessage
            |> Async.RunSynchronously
        printfn "Published message: %s | Partition: %i | Offset %i" msg res.partition res.offset

