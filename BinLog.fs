namespace BinLog

open System.Diagnostics

module BinLog =
    let read (filepath : string) : string =
        let proc = new Process()
        let cmd  = "mysqlbinlog"
        proc.StartInfo.UseShellExecute        <- false
        proc.StartInfo.FileName               <- cmd
        proc.StartInfo.Arguments              <- " -t --database=spreader " + filepath
        proc.StartInfo.CreateNoWindow         <- true
        proc.StartInfo.RedirectStandardOutput <- true
        proc.Start() |> ignore
        proc.StandardOutput.ReadToEnd()

