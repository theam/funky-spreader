type ProducerAlgebra<'next> =
    | WaitForMessage of              (string -> 'next)
    | PublishMessage of string     * 'next


let fmap f operation =
    match operation with
    | WaitForMessage(next) -> WaitForMessage(next >> f)
    | PublishMessage(message, next) -> PublishMessage(message, f next)



type ProducerProgram<'a> =
    | Pure of 'a
    | Free of ProducerAlgebra<ProducerProgram<'a>>



let returnT x = Pure x

let rec bindT f program =
    match program with
    | Pure x ->
        f x
    | Free instruction ->
        Free (fmap (bindT f) instruction)



type ProducerProgramBuilder() =
    member this.Return(x)  = returnT x
    member this.Bind(x, f) = bindT f x
    member this.Zero(x)    = returnT

let producerProgram = ProducerProgramBuilder()
