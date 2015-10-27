package org.bdgenomics.avocado.cli

import java.nio.file.Files

import org.apache.commons.configuration.HierarchicalConfiguration
import org.apache.commons.configuration.plist.PropertyListConfiguration
import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.GenotypeRDDFunctions
import org.bdgenomics.avocado.postprocessing.Postprocessor
import org.bdgenomics.avocado.stats.AvocadoConfigAndStats
import org.bdgenomics.avocado.Timers._
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => option, Argument }

object AvocadoFilter extends BDGCommandCompanion {
  val commandName = "Avocado Filter"
  val commandDescription = "Filters some variants"
  def apply(args: Array[String]) = {
    new AvocadoFilter(Args4j[AvocadoFilterArgs](args))
  }
}

class AvocadoFilterArgs extends Args4jBase with ParquetArgs {
  @Argument(metaVar = "VARIANTS", required = true, usage = "Unfiltered Variants", index = 0)
  var variantInput: String = _

  @Argument(metaVar = "FILTERED", required = true, usage = "Filtered Variants", index = 1)
  var variantOutput: String = _

  @Argument(metaVar = "CONFIG", required = true, usage = "avocado configuration file", index = 2)
  var configFile: String = _

  @option(name = "-debug", usage = "If set, prints a higher level of debug output.")
  var debug = false
}

class AvocadoFilter(protected val args: AvocadoFilterArgs) extends BDGSparkCommand[AvocadoFilterArgs] with Logging {

  val companion = AvocadoFilter

  def run(sc: SparkContext) = {
    /*
    val stream = Thread.currentThread.getContextClassLoader.getResourceAsStream(args.configFile)
    val tempPath = Files.createTempDirectory("config")
    val tempFilePath = tempPath.resolve("temp.properties")
    Files.copy(stream, tempFilePath)
    */

    // load config
    log.info("Filtering variants")
    val config: HierarchicalConfiguration = new PropertyListConfiguration(args.configFile)
    val stats = new AvocadoConfigAndStats(sc, args.debug, null, null)

    val genotypes: RDD[Genotype] = sc.loadGenotypes(args.variantInput)
    val variantContexts = new GenotypeRDDFunctions(genotypes).toVariantContext()
    val processedGenotypes = Postprocessor(variantContexts,
      "filterGaussianMixtureModel",
      "filterGaussianMixtureModel",
      stats, config).flatMap(variantContext => variantContext.genotypes)
    log.info("Writing calls to disk.")
    SaveVariants.time {
      processedGenotypes.adamParquetSave(args.variantOutput,
        args.blockSize,
        args.pageSize,
        args.compressionCodec,
        args.disableDictionaryEncoding)
    }
  }
}
