package software.amazon.ionhiveserde.integrationtest

interface TestConfiguration {
    abstract val shouldClose: Boolean
    abstract val shouldWait: Boolean
}

val TestSuiteConfiguration = object : TestConfiguration {
    override val shouldClose = false
    override val shouldWait = false
}

val NonSuiteConfiguration = object : TestConfiguration {
    override val shouldClose = true
    override val shouldWait = true
}
