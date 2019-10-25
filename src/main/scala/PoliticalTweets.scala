
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder
import edu.stanford.nlp.sentiment.SentimentUtils
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming._
import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.streaming._
import SentimentAnalysis._


object PoliticalTweets {

  def logging() = {
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
  }
  
  def main(args: Array[String]) {

    
    val consumerKey = "jJPngFGFCPfyZ5UZqEhKCxdaY"
    val consumerSecret = "q7NCaSbb7drqCxRQlUcGDgiOxkou3KE6aRuM8utOfYrhCwkhzA"
    val accessToken = "2572790058-ncEZd973r9eVjQlpA2rbf4agAXF3oBUeZAcibKf"
    val accessTokenSecret = "Q2eXbapT6gLKVl13hRXXWktS959hLhiqShVRghJ4SbfRm"
    
    val cb = new ConfigurationBuilder()
    cb.setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

    val tf = new TwitterFactory(cb.build)
    val twitter = tf.getInstance
    val auth = Some(twitter.getAuthorization)
    
    val conf = new SparkConf()

    conf.setAppName("PoliticalTweets").setMaster("local[2]")

    logging()

    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint("checkpoint")
// Creating the stream of tweets with the filter politics
    val politicalTweets = TwitterUtils.createStream(ssc, auth, Array("politics"))
// Filtering and keeping only english tweets
    val enPoliticalTweets = politicalTweets.filter(_.getLang() == "en")

// Connection with Cassandra
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
// Creation of Keyspace sentiment_keyspace and Table Sentiment_Count
    session.execute("CREATE KEYSPACE IF NOT EXISTS sentiment_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS sentiment_keyspace.sentiment_count (word text PRIMARY KEY, count int);")
    session.close()

//Creating tuples of (Sentiment, 1) --> i.e. (POSITIVE,1)
    val sentOne = enPoliticalTweets.map(x => (mainSentiment(x.getText).toString(), 1))

    def mappingFunc(key: String, value: Option[Int], state: State[Int]): (String, Int) = {
        if(state.exists()) {
            val count = state.get() + 1
            state.update(count)
            return(key,count)
        } 
        else {
            val count = 1
            state.update(count)
            return(key,count)
        }
    }

    val stateDStream = sentOne.mapWithState(StateSpec.function(mappingFunc _))

    // save to cassandra
    stateDStream.saveToCassandra("sentiment_keyspace", "sentiment_count", SomeColumns("word", "count"))


    ssc.start()
 
    ssc.awaitTermination()
    ssc.stop()
   }
}
