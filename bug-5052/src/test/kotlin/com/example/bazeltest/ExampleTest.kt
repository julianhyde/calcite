package com.example.bazeltest

import org.apache.calcite.test.DiffRepository
import org.junit.jupiter.api.Test

class ExampleTest {
    @Test
    fun testFixtureJarResource() {
        DiffRepository.lookup(ExampleTest::class.java)
    }
}
