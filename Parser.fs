namespace Parser

open FParsec
open FParsec.Primitives
open FParsec.CharParsers

module BinLog =

    type Parse<'a> = Parser<'a, unit>

    let maxInt = System.Int32.MaxValue

    let skipTillString str = skipCharsTillString str true maxInt

    let skipTillStringOrEof str : Parser<unit, _> =
        fun stream ->
            let mutable found = false
            stream.SkipCharsOrNewlinesUntilString(str, maxInt, &found) |> ignore
            Reply(())

    let binLogStart : Parse<unit> = skipString "BINLOG '" >>. spaces

    let binLogBody =
        charsTillString "'" true maxInt

    let skipToBinLogStartOrEof = skipTillStringOrEof "BINLOG '" 

    let allBinLogsParser =
        skipToBinLogStartOrEof
        >>. many (binLogStart
                  >>. binLogBody
                  .>> skipToBinLogStartOrEof)
        |>> List.choose Some
        .>> eof

    let parse s = run allBinLogsParser s
