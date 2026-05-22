package org.bsc.langgraph4j;

import org.bsc.langgraph4j.action.AsyncNodeAction;
import org.bsc.langgraph4j.state.AgentState;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.bsc.langgraph4j.GraphDefinition.END;
import static org.bsc.langgraph4j.GraphDefinition.START;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class Issue401Test {

    @Test
    public void replicateIssue()  {

        final var ex = assertThrows(IllegalStateException.class, () -> {
            final var stateGraph = new StateGraph<>(Map.of(), o -> null)
                    .addNode("A", AsyncNodeAction.node_async(state -> Map.of()))
                    .addNode("B", AsyncNodeAction.node_async(state -> Map.of()))
                    .addNode("C", AsyncNodeAction.node_async(state -> Map.of()))
                    .addNode("D", AsyncNodeAction.node_async(state -> Map.of()))
                    .addNode("E", AsyncNodeAction.node_async(state -> Map.of()))
                    .addNode("F", AsyncNodeAction.node_async(state -> Map.of()))
                    .addEdge(START, "A")
                    .addEdge("A", "B")
                    .addEdge("A", "C")
                    .addEdge("B", "D")
                    .addEdge("B", "E")
                    .addEdge("C", "F")
                    .addEdge("D", "F")
                    .addEdge("E", "F")
                    .addEdge("F", END)
                    .compile();
        });

        assertEquals( "Edge 'B' is parallel", ex.getMessage());
    }

    @Test
    public void resolveIssue() throws Exception {

        final var subGraphB = new StateGraph<>(Map.of(), AgentState::new )
                .addBeforeCallNodeHook( (( nodeId,  state,  config) -> {
                    System.out.println("before call node: " + nodeId);
                    return completedFuture(Map.of());
                }))
                .addAfterCallNodeHook( (( nodeId,  state,  config, lastResult) -> {
                    System.out.println("after call node: " + nodeId);
                    return completedFuture(lastResult);
                }))
                .addNode("B", AsyncNodeAction.node_async(state -> Map.of()))
                .addNode("D", AsyncNodeAction.node_async(state -> Map.of()))
                .addNode("E", AsyncNodeAction.node_async(state -> Map.of()))
                .addEdge(START, "B")
                .addEdge("B", "D")
                .addEdge("B", "E")
                .addEdge("D", END)
                .addEdge("E", END)
                .compile();

        final var graph = new StateGraph<>(Map.of(), AgentState::new)
            .addNode("A", AsyncNodeAction.node_async(state -> Map.of()))
            .addNode("BB", subGraphB )
            .addNode("C", AsyncNodeAction.node_async(state -> Map.of()))
            .addNode("F", AsyncNodeAction.node_async(state -> Map.of()))
            .addEdge(START, "A")
            .addEdge("A", "BB")
            .addEdge("A", "C")
            .addEdge("BB", "F")
            .addEdge("C", "F")
            .addEdge("F", END)
            .compile();

        final var diagram = graph.getGraph( GraphRepresentation.Type.MERMAID , "Solution Issue 401", false );

        System.out.println( diagram.content() );

        final var iterator = graph.stream( GraphInput.noArgs(), RunnableConfig.empty());

        for( var step : iterator ) {

            System.out.println("node: " + step.node());
        }
    }

}
