using Dotnet.Storm.Adapter.Components;
using System;
using System.Collections.Generic;

namespace Dotnet.Storm.Example
{
    public class EmitSentence : BaseSpout
    {
        private IDictionary<int, string> sentences;

        private Random random = new Random();

        public EmitSentence() : base()
        {
            sentences = new Dictionary<int, string>()
            {
                {0, "You can use asynchronous read operations to avoid these dependencies and their deadlock potential. Alternately, you can avoid the deadlock condition by creating two threads and reading the output of each stream on a separate thread." },
                {1, "To run this code, copy and paste the class into a Visual C# console application project that has been created in Visual Studio. By default, this project targets version 3.5 of the .NET Framework, and it has a reference to System.Core.dll and a using directive for System.Linq. If one or more of these requirements are missing from the project, you can add them manually." },
                {2, "The feedback system for this content will be changing soon. Old comments will not be carried over. If content within a comment thread is important to you, please save a copy. For more information on the upcoming change, we invite you to read our blog post." },
                {3, "The following code example creates an empty Dictionary<TKey,TValue> of strings with string keys and uses the Add method to add some elements. The example demonstrates that the Add method throws an ArgumentException when attempting to add a duplicate key." },
                {4, "The example uses the Item[TKey] property (the indexer in C#) to retrieve values, demonstrating that a KeyNotFoundException is thrown when a requested key is not present, and showing that the value associated with a key can be replaced." },
                {5, "The example shows how to use the TryGetValue method as a more efficient way to retrieve values if a program often must try key values that are not in the dictionary, and it shows how to use the ContainsKey method to test whether a key exists before calling the Add method." },
                {6, "The example shows how to enumerate the keys and values in the dictionary and how to enumerate the keys and values alone using the Keys property and the Values property." },
                {7, "The foreach statement of the C# language (for each in C++, For Each in Visual Basic) returns an object of the type of the elements in the collection. Since the Dictionary<TKey,TValue> is a collection of keys and values, the element type is not the type of the key or the type of the value. Instead, the element type is a KeyValuePair<TKey,TValue> of the key type and the value type. For example:" },
                {8, "The System.Collections.Generic namespace contains interfaces and classes that define generic collections, which allow users to create strongly typed collections that provide better type safety and performance than non-generic strongly typed collections." },
                {9, "Many of the generic collection types are direct analogs of nongeneric types. Dictionary<TKey,TValue> is a generic version of Hashtable; it uses the generic structure KeyValuePair<TKey,TValue> for enumeration instead of DictionaryEntry. List<T> is a generic version of ArrayList. There are generic Queue<T> and Stack<T> classes that correspond to the nongeneric versions. There are generic and nongeneric versions of SortedList<TKey,TValue>. Both versions are hybrids of a dictionary and a list. The SortedDictionary<TKey,TValue> generic class is a pure dictionary and has no nongeneric counterpart. The LinkedList<T> generic class is a true linked list and has no nongeneric counterpart." }
            };
        }

        private DateTime last;

        protected override void Next()
        {
            DateTime now = DateTime.Now;

            if ((now - last).Seconds > 20)
            {
                Storm.Emit(new List<object>() { sentences[random.Next(9)] });
                last = now;
            }
        }
    }
}
