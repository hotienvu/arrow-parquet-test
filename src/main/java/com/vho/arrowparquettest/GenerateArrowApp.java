package com.vho.arrowparquettest;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateArrowApp {
  private  static Logger LOG = LoggerFactory.getLogger(GenerateArrowApp.class);



  public static void main(String[] args) {
    LOG.info("start");
    GenerateArrowApp app = new GenerateArrowApp();
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    final int numPeople = 10_000_000;
    LOG.info("Generating {} people", numPeople);
    Person[] people = app.generateRandom(numPeople);
    LOG.info("Time = {}", stopWatch);
    LOG.info("Writing to arrow file ");
    app.writeToArrowFile(people);
    stopWatch.split();
    stopWatch.stop();
    LOG.info("Time = {}", stopWatch);
  }

  private void writeToArrowFile(Person[] people) {

  }

  private Person[] generateRandom(int numPeople) {
    Person[] res = new Person[numPeople];
    for (int i=0;i<numPeople;++i)
      res[i] = Person.randomPerson();
    return res;
  }
}
