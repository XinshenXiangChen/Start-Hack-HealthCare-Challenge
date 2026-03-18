from __future__ import annotations

import argparse
import os

from crewai import Agent, Crew, Task
from crewai_tools import CodeInterpreterTool
from langchain_community.llms import Ollama


def build_llm(model: str) -> Ollama:
    """
    Build an Ollama LLM client.

    In Docker, point this at your host Ollama with:
      -e OLLAMA_BASE_URL=http://host.docker.internal:11434
    """

    base_url = os.getenv("OLLAMA_BASE_URL")
    if base_url:
        return Ollama(model=model, base_url=base_url)
    return Ollama(model=model)


def run_fib_task(model: str = "llama3.1") -> str:
    """
    Example CrewAI task: generate fib(10) and write to fib.txt.
    Returns the Crew output.
    """

    local_llm = build_llm(model)

    coder_agent = Agent(
        role="Senior Python Developer",
        goal="Write and execute Python code to solve tasks",
        backstory=(
            "You are an expert coder. You use your Code Interpreter tool to run scripts "
            "and verify results."
        ),
        tools=[CodeInterpreterTool()],
        llm=local_llm,
        allow_code_execution=True,
    )

    task = Task(
        description=(
            "Calculate the first 10 numbers of the Fibonacci sequence and save them "
            "to a file named fib.txt"
        ),
        expected_output="A file named fib.txt containing the sequence.",
        agent=coder_agent,
    )

    crew = Crew(agents=[coder_agent], tasks=[task])
    return str(crew.kickoff())


def main() -> None:
    parser = argparse.ArgumentParser(description="Start Hack pipeline (Docker-friendly CLI)")
    parser.add_argument("--model", default="llama3.1", help="Ollama model name (default: llama3.1)")
    args = parser.parse_args()

    out = run_fib_task(model=args.model)
    print(out)


if __name__ == "__main__":
    main()