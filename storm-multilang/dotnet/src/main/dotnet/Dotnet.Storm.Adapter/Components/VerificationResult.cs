namespace Dotnet.Storm.Adapter.Components
{
    public class VerificationResult
    {
        private string reason;

        internal VerificationResult(bool error, string message)
        {
            IsError = error;
            reason = message;
        }

        public bool IsError { get; private set; }

        public string Reason {
            get
            {
                return ToString();
            }
            private set
            {
                reason = value;
            }
        }

        public override string ToString()
        {
            string prefix = IsError ? "Verification failed." : "Verification passed.";
            return string.Format("{0} {1}", prefix, reason);
        }
    }
}
