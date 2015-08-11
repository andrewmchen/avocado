package org.bdgenomics.avocado.postprocessing

import org.apache.commons.configuration.SubnodeConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.mllib.linalg.DenseVector
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.avocado.stats.AvocadoConfigAndStats
import org.bdgenomics.formats.avro.Genotype

private[postprocessing] object FilterGaussianMixtureModel extends PostprocessingStage {
  val stageName = "filterGaussianMixtureModel"

  def apply(variantContext: RDD[VariantContext],
            stats: AvocadoConfigAndStats,
            config: SubnodeConfiguration): RDD[VariantContext] = {
  }
}

class FilterGaussianMixtureModel(config: SubnodeConfiguration) extends GenotypeRDDFilter {
  va

  def filterGenotypes(genotypes: RDD[Genotype]): RDD[Genotype] = {

  }

  def toGenotypeVector(genotype: Genotype): GenotypeVector = {
    Option(genotype.getVariantCallingAnnotations)
  }
}

private[FilterGaussianMixtureModel] class GenotypeVector(genotype: Genotype,
                                                         values: Array[Double]) extends DenseVector(values) {

}



