package org.bdgenomics.avocado.postprocessing

import org.apache.commons.configuration.SubnodeConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.{Vectors, DenseVector}
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.avocado.stats.AvocadoConfigAndStats
import org.bdgenomics.formats.avro.{VariantCallingAnnotations, Genotype}

private[postprocessing] object FilterGaussianMixtureModel extends PostprocessingStage {
  val stageName = "filterGaussianMixtureModel"

  def apply(variantContext: RDD[VariantContext],
            stats: AvocadoConfigAndStats,
            config: SubnodeConfiguration): RDD[VariantContext] = {
  }
}

class FilterGaussianMixtureModel(config: SubnodeConfiguration) extends GenotypeRDDFilter {

  def filterGenotypes(genotypes: RDD[Genotype]): RDD[Genotype] = {
    val gaussianMixtureModel = generateGaussians(genotypes)

  }

  def generateGaussians(genotypes: RDD[Genotype]): GaussianMixtureModel = {
    // Filter of genotypes that don't have needed annotations
    // currently the annotations that we will use are:
    // mqRankSum, readPositionRankSum, rmsMapQ, variantQualityByDepth
    genotypes.filter(x => {
      val annotations = getAnnotations(x.getVariantCallingAnnotations)
      !annotations.contains(null)
    })
    val trainingVectors = genotypes.map(x => {
      val annotations = getAnnotations(x.getVariantCallingAnnotations)
      Vectors.dense(annotations.map(x => x.toDouble).toArray)
    })
    new GaussianMixture().setK(2).run(trainingVectors)
  }

  def getAnnotations(annotations: VariantCallingAnnotations): List[Float] = {
    List(annotations.getMqRankSum, annotations.getRmsMapQ, annotations.getReadPositionRankSum,
          annotations.getVariantQualityByDepth)
  }

}




