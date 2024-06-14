import gdb

def print_backtrace(fp, stack_bottom, stack_id):
    gdb.execute("set language c")
    print("coroutine {}, fp: {}, bottom: {}".format(stack_id, fp, stack_bottom))
    depth = 1
    while True:
        print("#{}".format(depth), end="  ")
        func_addr = gdb.parse_and_eval("*(uint64_t *)({} + 8)".format(fp))
        gdb.execute("info symbol {}".format(func_addr))
        print("        ", end='')
        gdb.execute("info line *({})".format(func_addr))
        if fp + 16 == stack_bottom:
            break
        fp = gdb.parse_and_eval("*(uint64_t *)({})".format(fp))
        depth += 1
    print()

max_coroutines = 65536

class ListPendingCoroutines(gdb.Command):
    '''list all pending coroutines'''
    def __init__(self):
        gdb.Command.__init__(self, 'co-list-pending', gdb.COMMAND_DATA, gdb.COMPLETE_NONE)

    def invoke(self, arg, from_tty):
        for i in range(1, max_coroutines):
            gdb.execute("set language rust")
            stack_top = gdb.parse_and_eval("uzfs::context::stack::STACKS.value[{}].value.stack_top".format(i))
            stack_id = gdb.parse_and_eval("uzfs::context::stack::STACKS.value[{}].value.stack_id".format(i))
            stack_bottom = gdb.parse_and_eval("uzfs::context::stack::STACKS.value[{}].value.stack_bottom".format(i))
            if stack_bottom == 0:
                return
            if stack_top != 0:
                print_backtrace(stack_top + 48, stack_bottom, stack_id)       

if __name__ == "__main__":
    ListPendingCoroutines()
