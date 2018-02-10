using Microsoft.Azure.Documents;
using System.IO;
using System.Text;

namespace CosmosBulkImport
{
    public interface IFileReader
    {
        void Load(string path);
        bool Validate();
        int GetDocumentCount();
        Document GetNextDocument();
    }

    public class FileReader : IFileReader
    {
        public void Load(string path)
        {
            _streamReader = File.OpenText(path);
            _isValid = int.TryParse(_streamReader.ReadLine(), out _documentCount);
        }

        public bool Validate()
        {
            return _isValid;
        }

        public int GetDocumentCount()
        {
            return _documentCount;
        }

        public Document GetNextDocument()
        {
            var data = _streamReader.ReadLine();
            if (data != null)
            {
                // build a Document instance from JSON text
                Document document = null;
                using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(data)))
                {
                    document = JsonSerializable.LoadFrom<Document>(memoryStream);
                }
                return document;
            }
            else
            {
                // we've reached the end of the file
                return null;
            }
        }

        private StreamReader _streamReader;
        private bool _isValid;
        private int _documentCount;
    }
}
