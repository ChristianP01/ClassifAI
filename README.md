# Scalable and Cloud Programming project
Multiclass-classification decision forest implemented with MapReduce C4.5 algorithm. <br>
Classification is done among 11 different topics, using a different tree for each topic.

The Dataproc service provided by Google Cloud Platform was leveraged for the creation of clusters having different configurations. Each cluster was assigned a relative job, so as to show the differences in performance and output as the input parameters change. <br>
Results and more details available in [notebook](ClassifAI.ipynb).

# Prerequisites
Scala version 2.12.15 <br>
Spark version 3.5.0
