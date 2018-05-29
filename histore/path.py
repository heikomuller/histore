# Copyright (C) 2018 New York University
# This file is part of OpenClean which is released under the Revised BSD License
# See file LICENSE for full license details.

"""Path expressions reference elements in the document tree."""


class Path(object):
    """Path expressions are sequences of node labels."""
    def __init__(self, path):
        """Initialize the path expression. Expects a list of element labels or
        a sting expressions with path components delimited by '/'.

        Raises ValueError if a path component is given that contains the '/'
        character.

        Parameters
        ----------
        path: string or list(string)
            Path expression is a list of element labels or a delimited string
            of element labels.
        """
        if isinstance(path, list):
            self.elements = list()
            for label in path:
                if '/' in label:
                    raise ValueError('invalid path component \'' + str(label) + '\'')
                self.elements.append(label)
        else:
            if path.strip() == '':
                self.elements = list()
            else:
                self.elements = path.strip().split('/')

    def __repr__(self):
        """Unambiguous string representation of this path object.

        Returns
        -------
        string
        """
        return 'Path(%s)' % ('/'.join(self.elements))

    def __str__(self):
        """Readable string representation of this path object.

        Returns
        -------
        string
        """
        return '/'.join(self.elements)

    def concat(self, path):
        """Returns a new path that is a concatenation of this path and the given
        path.

        Parameters
        ----------
        path: histore.path.Path

        Returns
        -------
        histore.document.path.Path
        """
        return Path(self.elements + path.elements)

    def extend(self, label):
        """Returns a new path that is an extension of the current path by
        appending the given label.

        Parameters
        ----------
        label: string

        Returns
        -------
        histore.document.path.Path
        """
        return Path(self.elements + [label])

    def first_element(self):
        """Get the first element in the path.

        Returns
        -------
        string
        """
        return self.elements[0]

    def get(self, index):
        """Get path component at the given index position.

        Parameters
        ----------
        index: int

        Returns
        -------
        string
        """
        return self.elements[index]

    def is_empty(self):
        """Shortcut to test if the length of the path expression is zero.

        Returns
        -------
        bool
        """
        return len(self.elements) == 0

    def length(self):
        """Shortcut for length of path.

        Returns
        -------
        int
        """
        return len(self.elements)

    def matches(self, path):
        """Returns True if the given path is the same as this path.

        Parameters
        ----------
        path: histore.document.path.Path

        Returns
        -------
        bool
        """
        if self.length() == path.length():
            for i in range(self.length()):
                if self.get(i) != path.get(i):
                    return False
            return True
        return False

    def subpath(self):
        """Get the subpath starting at the second position.

        Returns
        -------
        histore.document.path.Path
        """
        return Path(self.elements[1:])

    def to_key(self):
        """Get a string representation of the path that can be used as unique
        key.

        Returns
        -------
        string
        """
        return str(self)
