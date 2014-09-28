#!/usr/bin/env python
import zarafa
import matplotlib.pyplot as plt

def opt_args():
    parser = zarafa.parser('skpc')
    parser.add_option('--save', dest='save', action='store',  help='Save plot to file (png)')
    return parser.parse_args()

def b2m(bytes):
    return (bytes / 1024) / 1024

def main():
    options, args = opt_args()
    users = list(zarafa.Server(options).users())

    width = 0.35       # the width of the bars

    fig, ax = plt.subplots()
    ind = range(0, len(users))
    
     
    data = [b2m(user.store.size) for user in users]
    rects1 = ax.bar(ind, data, width, color='r')

    data = [len(list(user.store.folders())) for user in users]
    rects2 = ax.bar([offset + width for offset in ind], data, width, color='g')
        
    data =[sum(folder.count for folder in user.store.folders()) for user in users]
    rects3 = ax.bar([offset + width * 2 for offset in ind], data, width, color='b')

    ax.legend( (rects1[0], rects2[0], rects3[0]), ('Store size (Mb)', 'Folders', 'Items') )

    ax.set_ylabel('Values')
    ax.set_title('Store size, Folder, Items per user')
    ax.set_xticks([offset + width for offset in ind])
    ax.set_xticklabels([user.name for user in users])

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                    ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)
    autolabel(rects3)

    if options.save:
        plt.savefig(options.save)
    else:
        plt.show()





if __name__ == '__main__':
    main()
