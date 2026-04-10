package org.bsc.langgraph4j.agentexecutor;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import dev.langchain4j.data.message.UserMessage;
import org.bsc.langgraph4j.*;
import org.bsc.langgraph4j.action.InterruptionMetadata;
import org.bsc.langgraph4j.agent.AgentEx;
import org.bsc.langgraph4j.checkpoint.FileSystemSaver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class Issue375ITest {

    enum ApprovalEnum {
        APPROVE,
        REJECT
    }
    static class SearchTools {
        @Tool( name="weather_query", value="tool to get weather" )
        String weatherQuery(@P("weather day") String query) {

            return "the answer to: '%s' is: the weather is good".formatted(query);
        }

    }

    /**
     * test for issue <a href="https://github.com/langgraph4j/langgraph4j/issues/375">#375</a>
     */
    @ParameterizedTest
    @EnumSource( AgentEx.ApprovalState.class )
    public void testIssue375( AgentEx.ApprovalState approvalState ) throws Exception {
        final var path = Paths.get( "target", "checkpoint" );

        final var serializer = AgentExecutorEx.Serializers.STD.object();

        final var fileSystemSaver = new FileSystemSaver( path, serializer);
        final var build = CompileConfig.builder()
                .checkpointSaver(fileSystemSaver)
                .releaseThread(true)
                .build();

        final var executor = AgentExecutorEx.builder()
                .chatModel(AiModel.OLLAMA.chatModel("qwen3.5"))
                .stateSerializer(serializer)
                .toolsFromObject(new SearchTools())
                .approvalOn("weather_query", (nodeId, state) ->
                    InterruptionMetadata.builder(nodeId, state)
                            .addMetadata("label", "approve？")
                            .addMetadata("tool_name", "weather_query")
                            .build()
                )
                .build();

        final var graph = executor.compile(build);

        Map<String, Object> inputs = Map.of("messages", UserMessage.from("hows the weather on Monday?"));
        final var runnableConfig = RunnableConfig.builder()
                                        .threadId("23456")
                                        .build();
        final var stream = graph.stream(GraphInput.args(inputs), runnableConfig);

        final var result = stream.toCompletableFuture()
                                .thenApply(GraphResult::from)
                                .join();

        assertNotNull(result);
        assertTrue(result.isInterruptionMetadata());

        Map<String, Object> resume = switch( approvalState )  {
            case APPROVED -> Map.of("approval_result", "APPROVED");
            case REJECTED -> Map.of("approval_result", "REJECTED");
        };

        final var streamResume = graph.stream(GraphInput.resume(resume), runnableConfig);

        final var resultResume = streamResume.toCompletableFuture()
                .thenApply(GraphResult::from)
                .join();

        assertNotNull(resultResume);
        assertTrue(resultResume.isStateDataOrCheckpointSaverTag());


    }

}
