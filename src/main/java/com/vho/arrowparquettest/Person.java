package com.vho.arrowparquettest;


import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static com.vho.arrowparquettest.Util.pickRandom;

public class Person {
  private static final String[] FIRST_NAMES = new String[]{"John", "Jane", "Gerard", "Aubrey", "Amelia"};
  private static final String[] LAST_NAMES = new String[]{"Smith", "Parker", "Phillips", "Jones"};

  private static final MessageType schema = MessageTypeParser.parseMessageType("message people {\n" +
    "  optional binary firstName (UTF8);\n" +
    "  optional binary lastName (UTF8);\n" +
    "  optional int32 age;\n" +
    "optional group address {\n" +
    "  optional binary street (UTF8);\n" +
    "  optional int32 streetNumber;\n" +
    "  optional binary city (UTF8);\n" +
    "  optional int32 postalCode;\n" +
    "}\n" +
    "}");

  private static final org.apache.avro.Schema avroSchema = new AvroSchemaConverter().convert(schema);
  private static GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
  private final String firstName;
  private final String lastName;
  private final int age;
  private final Address address;

  static Person randomPerson() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    return new Person(
      pickRandom(FIRST_NAMES),
      pickRandom(LAST_NAMES),
      random.nextInt(0, 120),
      Address.randomAddress()
    );
  }

  public Person(String firstName, String lastName, int age, Address address) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.age = age;
    this.address = address;
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public int getAge() {
    return age;
  }


  public Address getAddress() {
    return address;
  }

  public static org.apache.avro.Schema  getAvroSchema() {
    return avroSchema;
  }

  public static Schema arrowSchema() {
    return new Schema(Arrays.asList(
      new Field("firstName", FieldType.nullable(new ArrowType.Utf8()), null),
      new Field("lastName", FieldType.nullable(new ArrowType.Utf8()), null),
      new Field("age", FieldType.nullable(new ArrowType.Int(32, false)), null),
      new Field("address", FieldType.nullable(new ArrowType.Struct()), Address.arrowSchema().getFields())
    ));
  }

  public GenericRecord toGenericRecord() {
    return builder.set("firstName", firstName)
      .set("lastName", lastName)
      .set("age", age)
      .set("address", address == null ? null : address.toGenericRecord())
      .build();
  }
}
