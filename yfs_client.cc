// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

yfs_client::yfs_client()
{
    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

int yfs_client::my_readlink(inum inum, std::string &buf, size_t size)
{
    printf("my_readlink: %lld, size=%ld bytes\n", inum, size);
    read(inum, size, 0, buf);
    return 0;
}

bool yfs_client::issymlink(inum inum) 
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error get attr\n");
        return false;
    } 

    if(a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symbolic link\n", inum);
        return true;
    }
    printf("issymlink: %lld is not a symbolic link\n", inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    // TODO:  revamp when it is time
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error get attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    }
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string content;
    ec->get(ino, content);
    extent_protocol::attr a;
    ec->getattr(ino, a);
    if (a.type == 0) {
        r = IOERR;
        return r;
    }

    std::cout << "> SET ATTR: inode=" << ino 
        << ", original size=" << a.size 
        << ", new size=" << size << std::endl;
    fflush(stdout);
    if (size > a.size) { 
        std::string padding(size - a.size, '\0');
        content += padding;
        a.size = size;
    } 
    else if (size < a.size) {
        content = content.substr(0, size);
        a.size = size;
    }
    ec->put(ino, content);

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    inum file_inum;
    r = lookup(parent, name, found, file_inum);
    if (r == OK && found) {
        r = EXIST;
    }
    else {
        std::list<dirent> entries;
        readdir(parent, entries);
        ec->create(extent_protocol::T_FILE, ino_out);
        dirent entry;
        entry.inum = ino_out;
        entry.name = name;
        entries.push_back(entry);

        // format directory entries
        std::string content;
        for (std::list<dirent>::iterator it = entries.begin(); it != entries.end(); it++) {
            std::ostringstream stream;
            stream << it->name << "\\:" << it->inum << "\\;";
            content += stream.str();
        }
        ec->put(parent, content);
    }
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    inum file_inum;
    r = lookup(parent, name, found, file_inum);
    if (r == OK && found) {
        r = EXIST;
    }
    else {
        std::list<dirent> entries;
        readdir(parent, entries);
        ec->create(extent_protocol::T_DIR, ino_out);
        dirent entry;
        entry.inum = ino_out;
        entry.name = name;
        entries.push_back(entry);
        
        // format directory entries
        std::string content;
        for (std::list<dirent>::iterator it = entries.begin(); it != entries.end(); it++) {
            std::ostringstream stream;
            stream << it->name << "\\:" << it->inum << "\\;";
            content += stream.str();
        }
        ec->put(parent, content);
    }
    
    return r;
}

int
yfs_client::create_symlink(inum parent, const char *name, mode_t mode, inum &ino_out) 
{
    int r = OK;

    bool found = false;
    inum symlink_inum;
    r = lookup(parent, name, found, symlink_inum);
    if (r == OK && found) {
        r = EXIST;
        return r;
    }
    else {
        std::list<dirent> entries;
        readdir(parent, entries);
        ec->create(extent_protocol::T_SYMLINK, ino_out);
        dirent entry;
        entry.inum = ino_out;
        entry.name = name;
        entries.push_back(entry);

        // format directory entries
        std::string content;
        for (std::list<dirent>::iterator it = entries.begin(); it != entries.end(); it++) {
            std::ostringstream stream;
            stream << it->name << "\\:" << it->inum << "\\;";
            content += stream.str();
        }
        ec->put(parent, content);
    }
    return r;
}

int 
yfs_client::write_symlink(inum ino, const char *linkname) 
{
    int r = OK;

    extent_protocol::attr a;
    std::string content(linkname);
    ec->getattr(ino, a);
    if (a.type != extent_protocol::T_SYMLINK) {
        r = NOENT;
        return r;
    }
    if (a.size > 0) {
        r = EXIST;
        return r;
    }
    ec->put(ino, content);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    found = false;
    std::list<dirent> entries = std::list<dirent>();
    readdir(parent, entries);
    for (std::list<dirent>::iterator it = entries.begin(); 
        it != entries.end(); it++) {
        if (it->name.compare(name) == 0) {
            ino_out = it->inum;
            found = true;
        }
    }
    printf("lookup: %s not found\n", name);
    fflush(stdout);
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;
    ec->get(dir, buf);
    std::string::size_type pos1 = 0;
    std::string::size_type pos2 = buf.find("\\;", pos1);
    while (pos2 != buf.npos && pos1 != pos2) {
        std::string ss = buf.substr(pos1, pos2);
        std::string::size_type subpos1 = 0;
        std::string::size_type subpos2 = ss.find("\\:");
        struct dirent entry;
        entry.name = ss.substr(subpos1, subpos2);
        subpos1 = subpos2 + 2;
        subpos2 = ss.size();
        std::string inum_str = ss.substr(subpos1, subpos2);
        std::istringstream stream(inum_str);
        stream >> entry.inum;
        list.push_back(entry);

        pos1 = pos2 + 2;
        pos2 = buf.find("\\;", pos1);
    }

    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string content;
    ec->get(ino, content);
    extent_protocol::attr a;
    ec->getattr(ino, a);
    if (a.type == 0 || a.type == extent_protocol::T_DIR) {
        r = NOENT;
        goto end;
    }
    std::cout << "> READ: inode=" << ino 
        << ", file size=" << a.size 
        << ", offset=" << off 
        << ", size=" << size << std::endl;
    fflush(stdout);
    if (off >= a.size) {
        data = "";
        goto end;
    }
    if (off + size > a.size) {
        data = content.substr(off, a.size - off);
        goto end;
    }
    data = content.substr(off, size);

end:
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    // data contains \0 before end, construct string with two params.
    std::string new_content(data, size);
    std::string old_content;
    extent_protocol::attr a;
    ec->getattr(ino, a);
    if (a.type != extent_protocol::T_FILE) {
        r = IOERR;
        return r;
    }
    std::cout << "> WRITE: inode=" << ino 
        << ", original size=" << a.size 
        << ", offset=" << off
        << ", size=" << size << std::endl;
    fflush(stdout);
    ec->get(ino, old_content);
    // fill the hole with zeros
    if (off > a.size) {
        std::string padding(off - a.size, '\0');
        old_content += padding + new_content;
    }
    // append the file
    else if (off == a.size) {
        old_content += new_content;
    }
    // write into middle of a file
    else {
        printf("<Write into middle of a file>\n File size: %d, offset: %ld\n", a.size, off);
        fflush(stdout);
        std::string sub = old_content.substr(0, off);
        sub += new_content;
        if (off + size < a.size) {
            sub += old_content.substr(off+size, a.size - off - size);
        }
        old_content = sub;
    }
    bytes_written = size;
    ec->put(ino, old_content);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    std::list<dirent> entries;
    readdir(parent, entries);

    bool found = false;
    for (std::list<dirent>::iterator it = entries.begin(); it != entries.end(); it++){
        if (it->name.compare(name) == 0) {
            found = true;
            ec->remove(it->inum);
            entries.erase(it);
            break;
        }
    }

    if (!found) {
        r = NOENT;
    }
    else {
        std::string content;
        for (std::list<dirent>::iterator it = entries.begin(); it != entries.end(); it++) {
            std::ostringstream stream;
            stream << it->name << "\\:" << it->inum << "\\;";
            content += stream.str();
        }
        ec->put(parent, content);
    }
    return r;
}

