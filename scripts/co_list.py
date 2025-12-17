import gdb

def print_backtrace(fp, stack_bottom, stack_id, last_pending):
    gdb.execute("set language c")
    print("coroutine {}, fp: {}, bottom: {}, last_pending_time: {}".format(stack_id, fp, stack_bottom, last_pending))
    depth = 1
    while True:
        print("#{}".format(depth), end="  ")
        func_addr = gdb.parse_and_eval("*(uint64_t *)({} + 8)".format(fp))
        gdb.execute("info symbol {}".format(func_addr))
        print("        ", end='')
        gdb.execute("info line *({})".format(func_addr))
        if fp + 16 >= stack_bottom:
            break
        fp = gdb.parse_and_eval("*(uint64_t *)({})".format(fp))
        if fp == 0:
            break
        depth += 1
    print()

max_coroutines = 262144

class ListPendingCoroutines(gdb.Command):
    '''list all pending coroutines'''
    def __init__(self):
        gdb.Command.__init__(self, 'co-list-pending', gdb.COMMAND_DATA, gdb.COMPLETE_NONE)

    def invoke(self, arg, from_tty):
        args = arg.split()
        if len(args) != 2 and args[0] != "aarch64" and args[0] != "amd64":
            print("Usage: co-list-pending <aarch64/amd64>")
            return
        offset = 144 if args[0] == "aarch64" else 48
        for i in range(1, max_coroutines):
            gdb.execute("set language rust")
            stack_top = gdb.parse_and_eval("uzfs::context::stack::STACKS.value[{}].value.stack_top".format(i))
            stack_id = gdb.parse_and_eval("uzfs::context::stack::STACKS.value[{}].value.stack_id".format(i))
            stack_bottom = gdb.parse_and_eval("uzfs::context::stack::STACKS.value[{}].value.stack_bottom".format(i))
            last_pending = gdb.parse_and_eval("uzfs::context::stack::STACKS.value[{}].value.last_pending".format(i))
            if stack_bottom == 0:
                return
            if stack_top != 0:
                print_backtrace(stack_top + offset, stack_bottom, stack_id, last_pending)       

if __name__ == "__main__":
    ListPendingCoroutines()
