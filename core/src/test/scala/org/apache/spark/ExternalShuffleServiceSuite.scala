/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

import scala.collection
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.SpanSugar._

import org.apache.spark.internal.config
import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.{ExecutorDiskUtils, ExternalBlockHandler, ExternalBlockStoreClient}
import org.apache.spark.storage.{BroadcastBlockId, RDDBlockId, ShuffleBlockId, ShuffleDataBlockId, ShuffleIndexBlockId, StorageLevel}
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * This suite creates an external shuffle server and routes all shuffle fetches through it.
 * Note that failures in this suite may arise due to changes in Spark that invalidate expectations
 * set up in `ExternalBlockHandler`, such as changing the format of shuffle files or how
 * we hash files into folders.
 */
class ExternalShuffleServiceSuite extends ShuffleSuite with BeforeAndAfterAll with Eventually {
  var server: TransportServer = _
  var transportContext: TransportContext = _
  var rpcHandler: ExternalBlockHandler = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores = 2)
    rpcHandler = new ExternalBlockHandler(transportConf, null)
    transportContext = new TransportContext(transportConf, rpcHandler)
    server = transportContext.createServer()

    conf.set(config.SHUFFLE_MANAGER, "sort")
    conf.set(config.SHUFFLE_SERVICE_ENABLED, true)
    conf.set(config.SHUFFLE_SERVICE_PORT, server.getPort)
  }

  override def afterAll(): Unit = {
    Utils.tryLogNonFatalError{
      server.close()
    }
    Utils.tryLogNonFatalError{
      rpcHandler.close()
    }
    Utils.tryLogNonFatalError{
      transportContext.close()
    }
    super.afterAll()
  }

  // This test ensures that the external shuffle service is actually in use for the other tests.
  test("using external shuffle service") {
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    sc.getConf.get(config.SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED) should equal(false)
    sc.env.blockManager.externalShuffleServiceEnabled should equal(true)
    sc.env.blockManager.blockStoreClient.getClass should equal(classOf[ExternalBlockStoreClient])

    // In a slow machine, one executor may register hundreds of milliseconds ahead of the other one.
    // If we don't wait for all executors, it's possible that only one executor runs all jobs. Then
    // all shuffle blocks will be in this executor, ShuffleBlockFetcherIterator will directly fetch
    // local blocks from the local BlockManager and won't send requests to ExternalShuffleService.
    // In this case, we won't receive FetchFailed. And it will make this test fail.
    // Therefore, we should wait until all executors are up
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

    val rdd = sc.parallelize(0 until 1000, 10)
      .map { i => (i, 1) }
      .reduceByKey(_ + _)

    rdd.count()
    rdd.count()

    // Invalidate the registered executors, disallowing access to their shuffle blocks (without
    // deleting the actual shuffle files, so we could access them without the shuffle service).
    rpcHandler.applicationRemoved(sc.conf.getAppId, false /* cleanupLocalDirs */)

    // Now Spark will receive FetchFailed, and not retry the stage due to "spark.test.noStageRetry"
    // being set.
    val e = intercept[SparkException] {
      rdd.count()
    }
    e.getMessage should include ("Fetch failure will not retry stage due to testing config")
  }

  test("SPARK-25888: using external shuffle service fetching disk persisted blocks") {
    val confWithRddFetchEnabled = conf.clone
      .set(config.SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED, true)
      .set(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED, true)
      .set(config.EXECUTOR_REMOVE_DELAY.key, "0s")
      .set(config.DRIVER_BIND_ADDRESS.key, Utils.localHostName())
    sc = new SparkContext("local-cluster[1,1,1024]", "test", confWithRddFetchEnabled)
    sc.env.blockManager.externalShuffleServiceEnabled should equal(true)
    sc.env.blockManager.blockStoreClient.getClass should equal(classOf[ExternalBlockStoreClient])
    try {
      val list = List[Int](1, 2, 3, 4)
      val broadcast = sc.broadcast(list)
      val rdd = sc.parallelize(0 until 100, 2)
        .map { i => (i, broadcast.value.size) }
        .persist(StorageLevel.DISK_ONLY)

      rdd.count()

      val blockId = RDDBlockId(rdd.id, 0)
      val bms = eventually(timeout(2.seconds), interval(100.milliseconds)) {
        val locations = sc.env.blockManager.master.getLocations(blockId)
        assert(locations.size === 2)
        assert(locations.map(_.port).contains(server.getPort),
          "external shuffle service port should be contained")
        locations
      }

      val dirManager = sc.env.blockManager.hostLocalDirManager
          .getOrElse(fail("No host local dir manager"))

      val promises = bms.map { case bmid =>
          val promise = Promise[File]()
          dirManager.getHostLocalDirs(bmid.host, bmid.port, Seq(bmid.executorId).toArray) {
            case scala.util.Success(res) => res.foreach { case (eid, dirs) =>
              val file = new File(ExecutorDiskUtils.getFilePath(dirs,
                sc.env.blockManager.subDirsPerLocalDir, blockId.name))
              promise.success(file)
            }
            case scala.util.Failure(error) => promise.failure(error)
          }
          promise.future
        }
      val filesToCheck = promises.map(p => ThreadUtils.awaitResult(p, Duration(2, "sec")))

      filesToCheck.foreach(f => {
        val parentPerms = Files.getPosixFilePermissions(f.getParentFile.toPath)
        assert(parentPerms.contains(PosixFilePermission.GROUP_WRITE))

        // On most operating systems the default umask will make this test pass
        // even if the permission isn't changed. To properly test this, run the
        // test with a umask of 0027
        val perms = Files.getPosixFilePermissions(f.toPath)
        assert(perms.contains(PosixFilePermission.OTHERS_READ))
      })

      sc.killExecutors(sc.getExecutorIds())

      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        val locations = sc.env.blockManager.master.getLocations(blockId)
        assert(locations.size === 1)
        assert(locations.map(_.port).contains(server.getPort),
          "external shuffle service port should be contained")
      }

      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        val locationStatusForLocalHost =
          sc.env.blockManager.master.getLocationsAndStatus(blockId, Utils.localHostName())
        assert(locationStatusForLocalHost.isDefined)
        assert(locationStatusForLocalHost.get.localDirs.isDefined)
        assert(locationStatusForLocalHost.get.locations.head.executorId == "0")
        assert(locationStatusForLocalHost.get.locations.head.host == Utils.localHostName())
      }

      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        val locationStatusForRemoteHost =
          sc.env.blockManager.master.getLocationsAndStatus(blockId, "<invalid-host>")
        assert(locationStatusForRemoteHost.isDefined)
        assert(locationStatusForRemoteHost.get.localDirs.isEmpty)
        assert(locationStatusForRemoteHost.get.locations.head.executorId == "0")
        assert(locationStatusForRemoteHost.get.locations.head.host == Utils.localHostName())
      }

      assert(sc.env.blockManager.getRemoteValues(blockId).isDefined)

      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        val broadcastBlockId = BroadcastBlockId(broadcast.id, "piece0")
        val locStatusForMemBroadcast =
          sc.env.blockManager.master.getLocationsAndStatus(broadcastBlockId, Utils.localHostName())
        assert(locStatusForMemBroadcast.isDefined)
        assert(locStatusForMemBroadcast.get.localDirs.isEmpty)
        assert(locStatusForMemBroadcast.get.locations.head.executorId == "driver")
        assert(locStatusForMemBroadcast.get.locations.head.host == Utils.localHostName())
      }

      val byteBuffer = ByteBuffer.wrap(Array[Byte](7))
      val bytes = new ChunkedByteBuffer(Array(byteBuffer))
      val diskBroadcastId = BroadcastBlockId(Long.MaxValue, "piece0")
      sc.env.blockManager.putBytes(diskBroadcastId, bytes, StorageLevel.DISK_ONLY,
        tellMaster = true)
      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        val locStatusForDiskBroadcast =
          sc.env.blockManager.master.getLocationsAndStatus(diskBroadcastId, Utils.localHostName())
        assert(locStatusForDiskBroadcast.isDefined)
        assert(locStatusForDiskBroadcast.get.localDirs.isDefined)
        assert(locStatusForDiskBroadcast.get.locations.head.executorId == "driver")
        assert(locStatusForDiskBroadcast.get.locations.head.host == Utils.localHostName())
      }

      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        val locStatusForDiskBroadcastForFetch =
          sc.env.blockManager.master.getLocationsAndStatus(diskBroadcastId, "<invalid-host>")
        assert(locStatusForDiskBroadcastForFetch.isDefined)
        assert(locStatusForDiskBroadcastForFetch.get.localDirs.isEmpty)
        assert(locStatusForDiskBroadcastForFetch.get.locations.head.executorId == "driver")
        assert(locStatusForDiskBroadcastForFetch.get.locations.head.host == Utils.localHostName())
      }

      // test unpersist: as executors are killed the blocks will be removed via the shuffle service
      rdd.unpersist(true)
      assert(sc.env.blockManager.getRemoteValues(blockId).isEmpty)
    } finally {
      rpcHandler.applicationRemoved(sc.conf.getAppId, true)
    }
  }

  test("SPARK-37618: external shuffle service removes shuffle blocks from deallocated executors") {
    for (enabled <- Seq(true, false)) {
      // Use local disk reading to get location of shuffle files on disk
      val confWithLocalDiskReading = conf.clone
        .set(config.SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED, true)
        .set(config.SHUFFLE_SERVICE_REMOVE_SHUFFLE_ENABLED, enabled)
        .set(config.EXECUTOR_REMOVE_DELAY.key, "0s")
      sc = new SparkContext("local-cluster[1,1,1024]", "test", confWithLocalDiskReading)
      sc.env.blockManager.externalShuffleServiceEnabled should equal(true)
      sc.env.blockManager.blockStoreClient.getClass should equal(classOf[ExternalBlockStoreClient])
      try {
        val rdd = sc.parallelize(0 until 100, 2)
          .map { i => (i, 1) }
          .repartition(1)

        rdd.count()

        val mapOutputs = sc.env.mapOutputTracker.getMapSizesByExecutorId(0, 0).toSeq

        val dirManager = sc.env.blockManager.hostLocalDirManager
          .getOrElse(fail("No host local dir manager"))

        val promises = mapOutputs.map { case (bmid, blocks) =>
          val promise = Promise[collection.Seq[File]]()
          dirManager.getHostLocalDirs(bmid.host, bmid.port, Seq(bmid.executorId).toArray) {
            case scala.util.Success(res) => res.foreach { case (eid, dirs) =>
              val files = blocks.flatMap { case (blockId, _, _) =>
                val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
                Seq(
                  ShuffleDataBlockId(shuffleBlockId.shuffleId, shuffleBlockId.mapId,
                    shuffleBlockId.reduceId).name,
                  ShuffleIndexBlockId(shuffleBlockId.shuffleId, shuffleBlockId.mapId,
                    shuffleBlockId.reduceId).name
                ).map { blockId =>
                  new File(ExecutorDiskUtils.getFilePath(dirs,
                    sc.env.blockManager.subDirsPerLocalDir, blockId))
                }
              }
              promise.success(files)
            }
            case scala.util.Failure(error) => promise.failure(error)
          }
          promise.future
        }
        val filesToCheck = promises.flatMap(p => ThreadUtils.awaitResult(p, Duration(2, "sec")))
        assert(filesToCheck.length == 4)
        assert(filesToCheck.forall(_.exists()))

        if (enabled) {
          filesToCheck.foreach(f => {
            val parentPerms = Files.getPosixFilePermissions(f.getParentFile.toPath)
            assert(parentPerms.contains(PosixFilePermission.GROUP_WRITE))

            // On most operating systems the default umask will make this test pass
            // even if the permission isn't changed. To properly test this, run the
            // test with a umask of 0027
            val perms = Files.getPosixFilePermissions(f.toPath)
            assert(perms.contains(PosixFilePermission.OTHERS_READ))
          })
        }

        sc.killExecutors(sc.getExecutorIds())
        eventually(timeout(2.seconds), interval(100.milliseconds)) {
          assert(sc.env.blockManager.master.getExecutorEndpointRef("0").isEmpty)
        }

        sc.cleaner.foreach(_.doCleanupShuffle(0, true))

        if (enabled) {
          assert(filesToCheck.forall(!_.exists()))
        } else {
          assert(filesToCheck.forall(_.exists()))
        }
      } finally {
        rpcHandler.applicationRemoved(sc.conf.getAppId, true)
        sc.stop()
      }
    }
  }

  test("SPARK-38640: memory only blocks can unpersist using shuffle service cache fetching") {
    for (enabled <- Seq(true, false)) {
      val confWithRddFetch =
        conf.clone.set(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED, enabled)
      sc = new SparkContext("local-cluster[1,1,1024]", "test", confWithRddFetch)
      sc.env.blockManager.externalShuffleServiceEnabled should equal(true)
      sc.env.blockManager.blockStoreClient.getClass should equal(classOf[ExternalBlockStoreClient])
      try {
        val rdd = sc.parallelize(0 until 100, 2)
          .map { i => (i, 1) }
          .persist(StorageLevel.MEMORY_ONLY)

        rdd.count()
        rdd.unpersist(true)
        assert(sc.persistentRdds.isEmpty)
      } finally {
        rpcHandler.applicationRemoved(sc.conf.getAppId, true)
        sc.stop()
      }
    }
  }
}
