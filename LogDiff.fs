namespace LogDiff

module LogDiff =

    let diff ( oldLog : string list ) ( newLog : string list) : string list =
        newLog
        |> Seq.skip oldLog.Length
        |> Seq.toList

