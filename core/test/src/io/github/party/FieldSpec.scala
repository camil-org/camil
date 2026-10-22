package io.github.party

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.github.party.handlers.FieldImpl

class FieldSpec extends AnyFlatSpec with Matchers:
  behavior of "Field extension methods"

  it should "compute sum correctly with local value" in:
    val field = FieldImpl("self", 5, Map("n1" -> 10, "n2" -> 20, "n3" -> 15))
    field.sum shouldBe 50

  it should "compute sum correctly with empty neighbors" in:
    val field = FieldImpl("self", 5, Map.empty[String, Int])
    field.sum shouldBe 5

  it should "compute sumWithoutSelf correctly" in:
    val field = FieldImpl("self", 5, Map("n1" -> 10, "n2" -> 20, "n3" -> 15))
    field.sumWithoutSelf(0) shouldBe 45

  it should "use default value for sumWithoutSelf with empty neighbors" in:
    val field = FieldImpl("self", 5, Map.empty[String, Int])
    field.sumWithoutSelf(100) shouldBe 100

  it should "compute max correctly with local value" in:
    val field = FieldImpl("self", 25, Map("n1" -> 10, "n2" -> 20, "n3" -> 15))
    field.max shouldBe 25

  it should "compute max correctly when neighbor has max value" in:
    val field = FieldImpl("self", 5, Map("n1" -> 10, "n2" -> 30, "n3" -> 15))
    field.max shouldBe 30

  it should "compute maxWithoutSelf correctly" in:
    val field = FieldImpl("self", 5, Map("n1" -> 10, "n2" -> 20, "n3" -> 15))
    field.maxWithoutSelf(Int.MinValue) shouldBe 20

  it should "use default value for maxWithoutSelf with empty neighbors" in:
    val field = FieldImpl("self", 5, Map.empty[String, Int])
    field.maxWithoutSelf(Int.MinValue) shouldBe Int.MinValue

  it should "compute min correctly with local value" in:
    val field = FieldImpl("self", 5, Map("n1" -> 10, "n2" -> 20, "n3" -> 15))
    field.min shouldBe 5

  it should "compute min correctly when neighbor has min value" in:
    val field = FieldImpl("self", 25, Map("n1" -> 10, "n2" -> 3, "n3" -> 15))
    field.min shouldBe 3

  it should "compute minWithoutSelf correctly" in:
    val field = FieldImpl("self", 5, Map("n1" -> 10, "n2" -> 20, "n3" -> 15))
    field.minWithoutSelf(Int.MaxValue) shouldBe 10

  it should "use default value for minWithoutSelf with empty neighbors" in:
    val field = FieldImpl("self", 5, Map.empty[String, Int])
    field.minWithoutSelf(Int.MaxValue) shouldBe Int.MaxValue

  it should "add two fields correctly using + operator" in:
    val field1: Field[Int] = FieldImpl("self1", 5, Map("n1" -> 10, "n2" -> 20))
    val field2: Field[Int] = FieldImpl("self2", 3, Map("n1" -> 7, "n2" -> 8))
    val result = (field1 + field2).asInstanceOf[FieldImpl[Int, String]]
    result.localValue shouldBe 8
    result.withoutSelf("n1") shouldBe 17
    result.withoutSelf("n2") shouldBe 28

  it should "subtract two fields correctly using - operator" in:
    val field1 = FieldImpl("self1", 10, Map("n1" -> 20, "n2" -> 30))
    val field2 = FieldImpl("self2", 3, Map("n1" -> 5, "n2" -> 8))
    val result = (field1 - field2).asInstanceOf[FieldImpl[Int, String]]
    result.localValue shouldBe 7
    result.withoutSelf("n1") shouldBe 15
    result.withoutSelf("n2") shouldBe 22

  it should "multiply two fields correctly using * operator" in:
    val field1 = FieldImpl("self1", 5, Map("n1" -> 10, "n2" -> 20))
    val field2 = FieldImpl("self2", 3, Map("n1" -> 2, "n2" -> 4))
    val result = (field1 * field2).asInstanceOf[FieldImpl[Int, String]]
    result.localValue shouldBe 15
    result.withoutSelf("n1") shouldBe 20
    result.withoutSelf("n2") shouldBe 80

  it should "work with Double values" in:
    val field = FieldImpl("self", 5.5, Map("n1" -> 10.5, "n2" -> 20.5))
    field.sum shouldBe 36.5

  it should "work with Long values" in:
    val field = FieldImpl("self", 5L, Map("n1" -> 10L, "n2" -> 20L))
    field.sum shouldBe 35L

  it should "handle fields with different neighbor sets in operations" in:
    val field1 = FieldImpl("self1", 5, Map("n1" -> 10))
    val field2 = FieldImpl("self2", 3, Map("n2" -> 7))
    val result = field1 + field2
    result.localValue shouldBe 8
    // operation involves only the common neighbors
    result.withoutSelf.size shouldBe 0

  it should "compute operations correctly with empty neighbor fields" in:
    val field1 = FieldImpl("self1", 10, Map.empty[String, Int])
    val field2 = FieldImpl("self2", 5, Map.empty[String, Int])
    val result = field1 + field2
    result.localValue shouldBe 15
    result.withoutSelf.isEmpty shouldBe true
end FieldSpec
