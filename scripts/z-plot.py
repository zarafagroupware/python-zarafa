import zarafa
import matplotlib.pyplot as plt

def opt_args():
    parser = zarafa.parser('skpc')
    parser.add_option('--store', dest='store', action='store_true',  help='Plots a graph with store sizes of users')
    parser.add_option('--folders', dest='folders', action='store_true',  help='Plots a graph with the number of folders per user')
    parser.add_option('--items', dest='items', action='store_true',  help='Plots a graph with the number of items per user')
    parser.add_option('--save', dest='save', action='store',  help='Save plot to file (png)')
    return parser.parse_args()

def b2m(bytes):
    return (bytes / 1024) / 1024

def main():
    options, args = opt_args()
    users = list(zarafa.Server(options).users())
    data = []

    fig, ax = plt.subplots() 

    if options.store:
        data = [b2m(user.store.size) for user in users]
        plt.ylabel('Store size (Mb)')
    elif options.folders:
        data = [len(list(user.store.folders())) for user in users]
        plt.ylabel('Folders)')
    elif options.items:
        # TODO: does a MAPI folder have an item that has the amount of items in it? since this is _very_ inefficient
        data = []
        for user in users:
            data.append(sum([len(list(folder.items())) for folder in user.store.folders()]))
        plt.ylabel('Items')
    else:
        return

    ax.plot(data)
    plt.xlabel('Users')
    labels = [item.get_text() for item in ax.get_xticklabels()]
    for i, user in enumerate(users):
        labels[i] = user.name
    ax.set_xticklabels(labels)

    if options.save:
        plt.savefig(options.save)
    else:
        plt.show()

if __name__ == '__main__':
    main()
