import cProfile
import pstats
from io import StringIO

import uvloop


async def run_test():
    """Run the all-in-one-performance test for profiling."""
    from examples.all_in_one_performance import main

    await main()


if __name__ == "__main__":
    # Profile the test
    profiler = cProfile.Profile()
    profiler.enable()

    uvloop.run(run_test())

    profiler.disable()

    # Print profiling results
    s = StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats(pstats.SortKey.CUMULATIVE)
    ps.print_stats(50)  # Top 50 functions to see more detail
    print(s.getvalue())

    # Also print by total time spent
    print("\n\n=== SORTED BY TOTAL TIME ===\n")
    s2 = StringIO()
    ps2 = pstats.Stats(profiler, stream=s2).sort_stats(pstats.SortKey.TIME)
    ps2.print_stats(30)
    print(s2.getvalue())
