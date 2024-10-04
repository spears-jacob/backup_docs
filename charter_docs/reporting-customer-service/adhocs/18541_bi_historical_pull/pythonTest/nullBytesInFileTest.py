if '\0' in open('all_logins.tsv').read():
    print "you have null bytes in your input file"
else:
    print "you don't"
