import luigi


class HelloWorld(luigi.Task):

    def run(self):
        print('{task} says: Hello world!'.format(task=self.__class__.__name__))


if __name__ == '__main__':
    luigi.run(['HelloWorld', '--workers', '1', '--local-scheduler'])
