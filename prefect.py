import asyncio
from prefect.client.orchestration import get_client
from prefect.server.schemas.filters import FlowRunFilter, FlowRunFilterState, FlowRunFilterStateName

# Function to list flow runs with a specific state
async def list_flow_runs_with_state(state):
    async with get_client() as client:
        flow_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(
                    name=FlowRunFilterStateName(any_=[state])
                )
            )
        )
    return flow_runs

# Function to delete flow runs
async def delete_flow_runs(flow_runs):
    async with get_client() as client:
        for flow_run in flow_runs:
            await client.delete_flow_run(flow_run_id=flow_run.id)

# Function to bulk delete flow runs based on their state
async def bulk_delete_flow_runs(state: str = "Completed"):
    flow_runs = await list_flow_runs_with_state(state)

    if len(flow_runs) == 0:
        print(f"There are no flow runs in state '{state}'")
        return

    print(f"There are {len(flow_runs)} flow runs with state {state}\n")

    for idx, flow_run in enumerate(flow_runs):
        print(f"[{idx + 1}] Flow '{flow_run.name}' with ID '{flow_run.id}'")

    if input("\n[Y/n] Do you wish to proceed: ") == "Y":
        print(f"Deleting {len(flow_runs)} flow runs...")
        await delete_flow_runs(flow_runs)
        print("Done.")
    else:
        print("Aborting...")

# Main function to run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(bulk_delete_flow_runs())
