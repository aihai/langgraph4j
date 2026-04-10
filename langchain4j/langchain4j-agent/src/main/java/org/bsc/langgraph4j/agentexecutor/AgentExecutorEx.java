package org.bsc.langgraph4j.agentexecutor;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.message.*;
import dev.langchain4j.invocation.InvocationContext;
import dev.langchain4j.invocation.InvocationParameters;
import org.bsc.langgraph4j.GraphStateException;
import org.bsc.langgraph4j.LG4JLoggable;
import org.bsc.langgraph4j.StateGraph;
import org.bsc.langgraph4j.action.*;
import org.bsc.langgraph4j.agent.AgentEx;
import org.bsc.langgraph4j.langchain4j.serializer.jackson.LC4jJacksonStateSerializer;
import org.bsc.langgraph4j.langchain4j.serializer.std.LC4jStateSerializer;
import org.bsc.langgraph4j.langchain4j.tool.LC4jToolService;
import org.bsc.langgraph4j.prebuilt.MessagesState;
import org.bsc.langgraph4j.serializer.StateSerializer;
import org.bsc.langgraph4j.state.Channel;
import org.bsc.langgraph4j.state.Channels;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static java.lang.String.format;
import static org.bsc.langgraph4j.state.AgentState.MARK_FOR_REMOVAL;
import static org.bsc.langgraph4j.state.AgentState.MARK_FOR_RESET;
import static org.bsc.langgraph4j.utils.CollectionsUtils.mapOf;
import static org.bsc.langgraph4j.utils.CollectionsUtils.mergeMap;

/**
 * Interface representing an Agent Executor (AKA ReACT agent).
 * This implementation make in evidence the tools execution using and action dispatcher node
 * <pre>
 *              ┌─────┐
 *              │start│
 *              └─────┘
 *                 |
 *              ┌─────┐
 *              │model│
 *              └─────┘
 *                |
 *          ┌─────────────────┐
 *          │action_dispatcher│
 *          └─────────────────┘_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 *          |                 \              \                    \
 *       ┌────┐         ┌─────────────┐ ┌─────────────┐      ┌─────────────┐
 *       │stop│         │ tool_name 1 │ │ tool_name 2 │......│ tool_name N │
 *       └────┘         └─────────────┘ └─────────────┘      └─────────────┘
 * </pre>
 */
public interface AgentExecutorEx extends LG4JLoggable {

    /**
     * Represents the state of an agent.
     */
    class State extends MessagesState<ChatMessage> {

        public static final String TOOL_EXECUTION_RESULTS = "tool_execution_results";

        static final Map<String, Channel<?>> SCHEMA = mergeMap(
                MessagesState.SCHEMA,
                Map.of(TOOL_EXECUTION_RESULTS, Channels.appender(ArrayList::new) ) );

        public static final String FINAL_RESPONSE = "agent_response";
        public static final String NEXT_ACTION = "next_action";

        /**
         * Constructs a new State with the given initialization data.
         *
         * @param initData the initialization data
         */
        public State(Map<String, Object> initData) {
            super(initData);
        }

        public List<ToolExecutionResultMessage> toolExecutionResults() {
            return this.<List<ToolExecutionResultMessage>>value(TOOL_EXECUTION_RESULTS)
                    .orElseThrow(() -> new RuntimeException("messages not found"));
        }

        public Optional<String> nextAction() {
            return value(NEXT_ACTION);
        }

        /**
         * Retrieves the agent final response.
         *
         * @return an Optional containing the agent final response if present
         */
        public Optional<String> finalResponse() {
            return value(FINAL_RESPONSE);
        }

        private  List<ToolExecutionRequest> toolExecutionRequestsByName(String actionName ) {
            return lastMessage()
                    .filter(m -> ChatMessageType.AI == m.type())
                    .map(AiMessage.class::cast)
                    .filter(AiMessage::hasToolExecutionRequests)
                    .map(AiMessage::toolExecutionRequests)
                    .map(requests -> requests.stream()
                            .filter(req -> Objects.equals(req.name(), actionName)).toList())
                    .orElseGet(List::of)
                    ;
        }

    }

    /**
     * Enum representing different serializers for the agent state.
     */
    enum Serializers {

        STD(new LC4jStateSerializer<>(State::new) ),
        JSON(new LC4jJacksonStateSerializer<>(State::new));

        private final StateSerializer<State> serializer;

        /**
         * Constructs a new Serializers enum with the specified serializer.
         *
         * @param serializer the state serializer
         */
        Serializers(StateSerializer<State> serializer) {
            this.serializer = serializer;
        }

        /**
         * Retrieves the state serializer.
         *
         * @return the state serializer
         */
        public StateSerializer<State> object() {
            return serializer;
        }
    }

    static AsyncNodeActionWithConfig<State> executeTool( LC4jToolService toolService, String actionName ) {

        return ( state, config ) -> {
            log.trace( "ExecuteTool" );

            var toolExecutionRequests = state.lastMessage()
                    .filter( m -> ChatMessageType.AI==m.type() )
                    .map( m -> (AiMessage)m )
                    .filter(AiMessage::hasToolExecutionRequests)
                    .map(AiMessage::toolExecutionRequests)
                    .map( requests -> requests.stream()
                            .filter( req -> Objects.equals(req.name(), actionName)).toList())
                    .orElseThrow(() -> new IllegalArgumentException("no tool execution request found!"))
                    ;

            final var context = InvocationContext.builder()
                    .invocationParameters( InvocationParameters.from(state.data()))
                    .build();

            return toolService.execute( toolExecutionRequests, context, "tool_execution_results")
                    .thenApply( Command::update );

        };
    }

    private static AsyncNodeActionWithConfig<State> dispatchTools(Set<String> approvals ) {
        return AsyncNodeActionWithConfig.node_async(( state, config ) -> {

            log.trace("DispatchTools");

            var toolExecutionRequests = state.lastMessage()
                    .filter( m -> ChatMessageType.AI==m.type() )
                    .map( m -> (AiMessage)m )
                    .filter(AiMessage::hasToolExecutionRequests)
                    .map(AiMessage::toolExecutionRequests);

            if( toolExecutionRequests.isEmpty() ) {
                return Map.of(State.FINAL_RESPONSE, "no tool execution request found!" );
            }

            var requests = toolExecutionRequests.get();

            return requests.stream()
                    .filter( request -> state.toolExecutionResults().stream()
                            .noneMatch( r -> Objects.equals(r.toolName(), request.name())))
                    .findFirst()
                    .map( result -> ( approvals.contains(result.name()) ?
                            "approval_%s".formatted(result.name()) :
                            result.name()))
                    .map( actionId -> Map.<String,Object>of( State.NEXT_ACTION, actionId ))
                    .orElseGet( () ->  mapOf(State.MESSAGES_STATE,  state.toolExecutionResults(),
                            State.TOOL_EXECUTION_RESULTS, MARK_FOR_RESET, /* reset results */
                            State.NEXT_ACTION, MARK_FOR_REMOVAL  /* remove element */ ));
        });
    }

    private static AsyncCommandAction<State> approvalAction() {
        return (state, config) -> {
            var result = new CompletableFuture<Command>();

            if( state.value( AgentEx.APPROVAL_RESULT_PROPERTY ).isEmpty() ) {
                result.completeExceptionally( new IllegalStateException(format("resume property '%s' not found!", AgentEx.APPROVAL_RESULT_PROPERTY) ));
                return result;
            }

            var resumeState = state.<String>value( AgentEx.APPROVAL_RESULT_PROPERTY )
                    .orElseThrow( () -> new IllegalStateException( "resume property '%s' not found!".formatted(AgentEx.APPROVAL_RESULT_PROPERTY) ));

            // APPROVED
            if( Objects.equals( resumeState, AgentEx.ApprovalState.APPROVED.name() )) {
                result.complete( new Command( resumeState,
                        Map.of(AgentEx.APPROVAL_RESULT_PROPERTY, MARK_FOR_REMOVAL)));

            }
            else {
                // DENIED
                final var actionName = state.nextAction()
                        .map( v -> v.replace("approval_", "") )
                        .orElseThrow( () -> new IllegalStateException("no next action found!"));

                final var tools = state.toolExecutionRequestsByName( actionName );

                if(tools.isEmpty())  {
                    throw new IllegalStateException("no tool execution request found!");
                }

                var toolResponses = tools.stream().map( toolRequest ->
                        ToolExecutionResultMessage.from( toolRequest, "execution has been DENIED!")
                ).toList();

                result.complete( new Command( AgentEx.CALL_MODEL_NODE,
                        Map.of( State.MESSAGES_STATE, toolResponses ,
                                State.TOOL_EXECUTION_RESULTS, "execution has been DENIED!",
                                AgentEx.APPROVAL_RESULT_PROPERTY, MARK_FOR_REMOVAL)));

            }
            return result;
        };
    }

    private static AsyncCommandAction<AgentExecutorEx.State> shouldContinue() {
        return AsyncCommandAction.command_async( (state, config ) ->
                state.finalResponse()
                        .map(res -> new Command(AgentEx.END_LABEL))
                        .orElse(new Command(AgentEx.CONTINUE_LABEL)) );
    }

    private static AsyncCommandAction<State> dispatchAction() {
        return AsyncCommandAction.command_async( (state, config ) ->
                state.nextAction()
                        .map( Command::new )
                        .orElseGet( () -> new Command(AgentEx.CALL_MODEL_NODE ) ));

    }

    /**
     * Builder class for constructing a graph of agent execution.
     */
    class Builder extends AgentExecutorBuilder<State,Builder>  {

        private final Map<String,AgentEx.ApprovalNodeAction<ChatMessage,State>> approvals = new LinkedHashMap<>();

        public Builder approvalOn( String actionId, BiFunction<String, State, InterruptionMetadata<State>> interruptionMetadataProvider  ) {
            var action = AgentEx.ApprovalNodeAction.<ChatMessage,AgentExecutorEx.State>builder()
                    .interruptionMetadataProvider( interruptionMetadataProvider )
                    .build();

            approvals.put( actionId, action  );
            return this;
        }

        /**
         * Builds the state graph.
         *
         * @return the constructed StateGraph
         * @throws GraphStateException if there is an error in the graph state
         */
        public StateGraph<State> build() throws GraphStateException {

            if (streamingChatModel != null && chatModel != null) {
                throw new IllegalArgumentException("chatLanguageModel and streamingChatLanguageModel are mutually exclusive!");
            }
            if (streamingChatModel == null && chatModel == null) {
                throw new IllegalArgumentException("a chatLanguageModel or streamingChatLanguageModel is required!");
            }

            if (stateSerializer == null) {
                stateSerializer = Serializers.STD.object();
            }

            var tools = toolMap();

            final LC4jToolService toolService = new LC4jToolService(tools);

            return AgentEx.<ChatMessage, State, ToolSpecification>builder()
                    .stateSerializer( stateSerializer )
                    .schema( State.SCHEMA )
                    .toolName(ToolSpecification::name)
                    .callModelAction( new CallModel<>(this) )
                    .dispatchToolsAction( dispatchTools( approvals.keySet() ) )
                    .executeToolFactory( ( toolName ) -> executeTool( toolService, toolName ) )
                    .shouldContinueEdge( shouldContinue() )
                    .approvalActionEdge( approvalAction() )
                    .dispatchActionEdge( dispatchAction() )
                    .build( tools.keySet(), approvals )
                    ;
        }
    }

    /**
     *
     * @return a new Builder
     */
    static Builder builder() {
        return new Builder();
    }

}
