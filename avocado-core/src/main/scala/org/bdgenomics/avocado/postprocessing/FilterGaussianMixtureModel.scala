package org.bdgenomics.avocado.postprocessing

import org.apache.commons.configuration.SubnodeConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.mllib.clustering.{ GaussianMixture, GaussianMixtureModel }
import org.apache.spark.mllib.linalg.{ Vectors, DenseVector }
import org.bdgenomics.adam.models.{ ReferencePosition, VariantContext }
import org.bdgenomics.avocado.stats.AvocadoConfigAndStats
import org.bdgenomics.formats.avro.{ VariantCallingAnnotations, Genotype }

private[postprocessing] object FilterGaussianMixtureModel extends PostprocessingStage {
  val stageName = "filterGaussianMixtureModel"

  def apply(variantContext: RDD[VariantContext],
            stats: AvocadoConfigAndStats,
            config: SubnodeConfiguration): RDD[VariantContext] = {
    new FilterGaussianMixtureModel(config).filter(variantContext)
  }
}

class FilterGaussianMixtureModel(config: SubnodeConfiguration) extends GenotypeRDDFilter with Logging {

  def filterGenotypes(genotypes: RDD[(ReferencePosition, Genotype)]): RDD[(ReferencePosition, Genotype)] = {
    val gaussianMixtureModel = generateGaussians(genotypes.map(x => x._2))
    val zipped = gaussianMixtureModel.gaussians.zip(gaussianMixtureModel.weights)
    for (zipVal <- zipped) {
      log.info(s"mu ${zipVal._1.mu}, cov ${zipVal._1.sigma}")
    }
    genotypes
  }

  def generateGaussians(genotypes: RDD[Genotype]): GaussianMixtureModel = {
    // Filter of genotypes that don't have needed annotations
    // currently the annotations that we will use are:
    // mqRankSum, readPositionRankSum, rmsMapQ, variantQualityByDepth
    log.info("filtering " + genotypes.count())
    genotypes.filter(x => {
      val annotations = getAnnotations(x.getVariantCallingAnnotations)
      !annotations.contains(None)
    })
    val trainingVectors = genotypes.map(x => {
      val annotations = getAnnotations(x.getVariantCallingAnnotations)
      Vectors.dense(annotations.flatten.map(x => x.toDouble).toArray)
    })
    new GaussianMixture().setK(2).run(trainingVectors)
  }

  def getAnnotations(annotations: VariantCallingAnnotations): List[Option[Float]] = {
    val values = Array(annotations.getMqRankSum, annotations.getRmsMapQ, annotations.getReadPositionRankSum,
      annotations.getVariantQualityByDepth).map(x => Option(x))
    values.map(x => x.map(y => y.toFloat)).toList
  }
}

