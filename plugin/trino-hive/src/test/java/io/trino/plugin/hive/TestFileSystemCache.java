/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.authentication.ImpersonatingHdfsAuthentication;
import io.trino.plugin.hive.authentication.SimpleHadoopAuthentication;
import io.trino.plugin.hive.authentication.SimpleUserNameProvider;
import io.trino.plugin.hive.fs.TrinoFileSystemCache;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestFileSystemCache
{
    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        // From https://maven.apache.org/surefire/maven-surefire-plugin/examples/fork-options-and-parallel-execution.html
        // The default setting is forkCount=1/reuseForks=true, which means that maven-surefire-plugin creates one new
        // JVM process to execute all tests in one Maven module.
        FileSystem.closeAll();
    }

    @Test
    public void testFileSystemCache()
            throws IOException
    {
        FileSystem.closeAll();
        HdfsEnvironment environment = new HdfsEnvironment(
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), ImmutableSet.of()),
                new HdfsConfig(),
                new ImpersonatingHdfsAuthentication(new SimpleHadoopAuthentication(), new SimpleUserNameProvider()));
        ConnectorIdentity userId = ConnectorIdentity.ofUser("user");
        ConnectorIdentity otherUserId = ConnectorIdentity.ofUser("other_user");
        FileSystem fs1 = getFileSystem(environment, userId);
        FileSystem fs2 = getFileSystem(environment, userId);
        assertSame(fs1, fs2);

        FileSystem fs3 = getFileSystem(environment, otherUserId);
        assertNotSame(fs1, fs3);

        FileSystem fs4 = getFileSystem(environment, otherUserId);
        assertSame(fs3, fs4);

        FileSystem.closeAll();

        FileSystem fs5 = getFileSystem(environment, userId);
        assertNotSame(fs5, fs1);
    }

    @Test
    public void testFileSystemCacheException() throws IOException
    {
        FileSystem.closeAll();
        TrinoFileSystemCache.checkUser.set(1);
        HdfsEnvironment environment = new HdfsEnvironment(
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), ImmutableSet.of()),
                new HdfsConfig(),
                new ImpersonatingHdfsAuthentication(new SimpleHadoopAuthentication(), new SimpleUserNameProvider()));

        int numUsers = 1000;
        for (int i = 0; i < numUsers; ++i) {
            assertEquals(TrinoFileSystemCache.INSTANCE.getFileSystemCacheStats().getCacheSize(), i);
            getFileSystem(environment, ConnectorIdentity.ofUser("user" + String.valueOf(i)));
        }
        System.err.println(TrinoFileSystemCache.INSTANCE);
        TrinoFileSystemCache.checkUser.set(0);

        assertEquals(TrinoFileSystemCache.INSTANCE.getFileSystemCacheStats().getCacheSize(), 1000);
        assertEquals(TrinoFileSystemCache.INSTANCE.getCacheSize(), 1000);

        try {
            getFileSystem(environment, ConnectorIdentity.ofUser("user" + String.valueOf(1000)));
            fail("Should have thrown IOException from above");
        }
        catch (IOException e) {
            assertEquals(e.getMessage(), "FileSystem max cache size has been reached: 1000");
        }
    }

    @Test
    public void testFileSystemCacheConcurrency() throws InterruptedException, ExecutionException, IOException
    {
        int numThreads = 20;
        List<Callable<Void>> callableTasks = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            callableTasks.add(
                    new CreateAndConsumeFileSystems(
                            new SplittableRandom(i),
                            10,
                            1000,
                            fs -> fs.close() /* triggers fscache.remove() */));
        }

        FileSystem.closeAll();

        TrinoFileSystemCache.checkUser.set(1);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        assertEquals(TrinoFileSystemCache.INSTANCE.getFileSystemCacheStats().getCacheSize(), 0);
        List<Future<Void>> futures = executor.invokeAll(callableTasks);
        for (Future<Void> fut : futures) {
            fut.get();
        }
        executor.shutdown();
        System.err.println(TrinoFileSystemCache.INSTANCE);
        TrinoFileSystemCache.checkUser.set(0);
        assertEquals(TrinoFileSystemCache.INSTANCE.getFileSystemCacheStats().getCacheSize(), 0, "Cache size is non zero");
        assertEquals(TrinoFileSystemCache.INSTANCE.getCacheSize(), 0, "cacheSize is non zero");
    }

    private static FileSystem getFileSystem(HdfsEnvironment environment, ConnectorIdentity identity)
            throws IOException
    {
        return environment.getFileSystem(identity, new Path("/"), newEmptyConfiguration());
    }

    @FunctionalInterface
    public interface FileSystemConsumer
    {
        void consume(FileSystem fileSystem) throws IOException;
    }

    // A callable that creates (and consumes) filesystem objects X times for Y users
    public static class CreateAndConsumeFileSystems  // JITS CreateFileSystemAndConsume
            implements Callable<Void>
    {
        private final SplittableRandom random;
        private final int numUsers;
        private final int numGetCallsPerInvocation;
        private final FileSystemConsumer consumer;

        private HdfsEnvironment environment = new HdfsEnvironment(
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), ImmutableSet.of()),
                new HdfsConfig(),
                new ImpersonatingHdfsAuthentication(new SimpleHadoopAuthentication(), new SimpleUserNameProvider()));

        CreateAndConsumeFileSystems(SplittableRandom random, int numUsers, int numGetCallsPerInvocation, FileSystemConsumer consumer)
        {
            this.random = random;
            this.numUsers = numUsers;
            this.numGetCallsPerInvocation = numGetCallsPerInvocation;
            this.consumer = consumer;
        }

        @Override
        public Void call() throws IOException
        {
            for (int i = 0; i < numGetCallsPerInvocation; ++i) {
                FileSystem fs = getFileSystem(environment, ConnectorIdentity.ofUser("user" + String.valueOf(random.nextInt(numUsers))));
                consumer.consume(fs);
            }
            return null;
        }
    }
}
