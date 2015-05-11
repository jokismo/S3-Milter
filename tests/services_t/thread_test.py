from threading import Thread


def thread_tester():
    while True:
        a.append(1)
        if len(a) == 10:
            break
        print b.get()


class Tester(object):

    def __init__(self):
        self.f = 0
        self.a = a

    def get(self):
        self.f += 1
        return self.f


if __name__ == '__main__':
    a = []
    b = Tester()
    conn_user_thread = Thread(target=thread_tester)
    conn_user_thread.start()
    while True:
        print b.get()
        if len(a) == 10:
            break
    conn_user_thread.join()