type ConsumerAlgebra<'next> =
    | ReadBinLog     of FilePath   * (string -> 'next)
    | DiffBinLog     of string     * (string -> 'next)
    | SendToProducer of string     * 'next



let fmap f operation =
    match operation with
    | ReadBinLog(filePath, next)      -> ReadBinLog(filePath, next >> f)
    | DiffBinLog(currentBinLog, next) -> DiffBinLog(currentBinLog, next >> f)
    | SendToProducer(message, next)   -> SendToProducer(message, f next)



type ConsumerProgram<'a> =
    | Pure of 'a
    | Free of ConsumerAlgebra<ConsumerProgram<'a>>



let returnT x = Pure x

let rec bindT f program =
    match program with
    | Pure x ->
        f x
    | Free instruction ->
        Free (fmap (bindT f) instruction)



type ConsumerProgramBuilder() =
    member this.Return(x)  = returnT x
    member this.Bind(x, f) = bindT f x
    member this.Zero(x)    = returnT

let consumerProgram = ConsumerProgramBuilder()
