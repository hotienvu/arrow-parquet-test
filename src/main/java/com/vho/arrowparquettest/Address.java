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

public class Address {
  private static final String[] STREETS = new String[]{
    "Halloway",
    "Sunset Boulvard",
    "Wall Street",
    "Secret Passageway"
  };
  private static final String[] CITIES = new String[]{
    "Brussels",
    "Paris",
    "London",
    "Amsterdam"
  };

  private static final MessageType schema = MessageTypeParser.parseMessageType(
    "message address {\n" +
      "  optional binary street (UTF8);\n" +
      "  optional int32 streetNumber;\n" +
      "  optional binary city (UTF8);\n" +
      "  optional int32 postalCode;\n" +
      "}");

  private static final org.apache.avro.Schema avroSchema = new AvroSchemaConverter().convert(schema);
  private static final GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);


  private final String street;
  private final int streetNumber;
  private final String city;
  private final int postalCode;

  static Address randomAddress() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    return new Address(
      pickRandom(STREETS),
      random.nextInt(1, 3000),
      pickRandom(CITIES),
      random.nextInt(1000, 10000)
    );
  }

  public Address(String street, int streetNumber, String city, int postalCode) {
    this.street = street;
    this.streetNumber = streetNumber;
    this.city = city;
    this.postalCode = postalCode;
  }

  public static org.apache.arrow.vector.types.pojo.Schema arrowSchema() {
    return new Schema(Arrays.asList(
      new Field("street", FieldType.nullable(new ArrowType.Utf8()), null),
      new Field("streetNumber", FieldType.nullable(new ArrowType.Int(32, false)), null),
      new Field("city", FieldType.nullable(new ArrowType.Utf8()), null),
      new Field("postalCode", FieldType.nullable(new ArrowType.Int(32, false)), null)
    ));
  }


  public String getStreet() {
    return street;
  }

  public int getStreetNumber() {
    return streetNumber;
  }

  public String getCity() {
    return city;
  }

  public int getPostalCode() {
    return postalCode;
  }

  public GenericRecord toGenericRecord() {
    return builder.set("street", street)
      .set("streetNumber", streetNumber)
      .set("city", city)
      .set("postalCode", postalCode)
      .build();
  }
}
